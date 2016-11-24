import authService.ClientAuth
import com.twitter.finagle.{Service, ServiceTimeoutException}
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service.{Backoff, RetryExceptionsFilter, RetryPolicy}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Throw, Time, Try, Future => TwitterFuture}
import com.twitter.conversions.time._
import org.apache.curator.retry.ExponentialBackoffRetry
import resource.ConfigClient
import transactionService.client.ClientTransaction
import zooKeeper.ZKLeaderClient
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Transaction}


class TransactionZooKeeperClient(private val config: ConfigClient) {
  import config._

  private val zKLeaderClient = new ZKLeaderClient(zkEndpoints, zkTimeoutSession, zkTimeoutConnection,
    new ExponentialBackoffRetry(zkTimeoutBetweenRetries, zkRetriesMax), zkPrefix)
  zKLeaderClient.start

  private val clientAuth = new ClientAuth(authAddress, authTimeoutConnection, authTimeoutExponentialBetweenRetries)
  @volatile private var token = Await.result(clientAuth.authenticate(login, password))
  private val retryConditionToken: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(error) => error match {
      case e: ServiceTimeoutException => true
      case e: com.twitter.finagle.ChannelWriteException => true
      case e =>
        val messageToParse = e.getMessage
        Logger.get().log(Level.ERROR, messageToParse)
        if (messageToParse.contains(transactionService.exception.Throwables.tokenInvalidExceptionMessage)) {
          token = Await.result(clientAuth.authenticate(login, password))
          true
        } else false
    }
    case _ => false
  }
  private val retryPolicyToken = RetryPolicy.backoff(Backoff.equalJittered(300.milliseconds, 10.seconds))(retryConditionToken)
  private def retryFilterToken[Req, Rep] = new RetryExceptionsFilter[Req, Rep](retryPolicyToken, HighResTimer.Default)

  private val AddressToTransactionServiceServer = new scala.collection.concurrent.TrieMap[String, ClientTransaction]()
  private def getClientTransaction = {
    val master = zKLeaderClient.master.get
    AddressToTransactionServiceServer.putIfAbsent(master, new ClientTransaction(master))
    AddressToTransactionServiceServer(master)
  }

  private case class Stream(override val name: String, override val partitions: Int, override val description:Option[String])
    extends transactionService.rpc.Stream

  def putStream(stream: String, partitions: Int, description: Option[String]): TwitterFuture[Boolean] = {
    val streamService = new Service[Stream, Boolean] {
      override def apply(request: Stream): TwitterFuture[Boolean] = {
        getClientTransaction.putStream(token, request.name, request.partitions, request.description)}
    }
    val requestChain = retryFilterToken.andThen(streamService)
    requestChain(Stream(stream,partitions,description))
  }

  def putStream(stream: Stream): TwitterFuture[Boolean] = {
    val streamService = new Service[Stream, Boolean] {
      override def apply(request: Stream): TwitterFuture[Boolean] = {
        getClientTransaction.putStream(token, request.name, request.partitions, request.description)}
    }
    val requestChain = retryFilterToken.andThen(streamService)
    requestChain(stream)
  }

  def delStream(stream: String): TwitterFuture[Boolean] = {
    val streamService = new Service[String, Boolean] {
      override def apply(request: String): TwitterFuture[Boolean] = {
        getClientTransaction.delStream(token, request)
      }
    }
    val requestChain = retryFilterToken.andThen(streamService)
    requestChain(stream)
  }

  def delStream(stream: Stream): TwitterFuture[Boolean] = {
    val streamService = new Service[Stream, Boolean] {
      override def apply(request: Stream): TwitterFuture[Boolean] = {
        getClientTransaction.delStream(token, request.name)
      }
    }
    val requestChain = retryFilterToken.andThen(streamService)
    requestChain(stream)
  }

  private case class ProducerTransactionWrapper(override val stream: String,
                                         override val partition: Int,
                                         override val transactionID: Long,
                                         override val state: transactionService.rpc.TransactionStates,
                                         override val quantity: Int,
                                         override val timestamp: Long,
                                         override val tll: Long
                                        ) extends transactionService.rpc.ProducerTransaction

  private case class ConsumerTransactionWrapper(override val stream: String,
                                         override val partition: Int,
                                         override val transactionID: Long,
                                         override val name: String
                                        ) extends transactionService.rpc.ConsumerTransaction


  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): TwitterFuture[Boolean] =
  {
    val transactionService = new Service[Seq[Transaction], Boolean] {
      override def apply(request: Seq[Transaction]): TwitterFuture[Boolean] = {
        getClientTransaction.putTransactions(token, request)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionService)

    val txns =
      producerTransactions.map(txn => new Transaction {
      override def producerTransaction: Option[ProducerTransaction] = Some(txn)
      override def consumerTransaction: Option[ConsumerTransaction] = None
    }) ++ consumerTransactions.map(txn => new Transaction {
      override def producerTransaction: Option[ProducerTransaction] = None
      override def consumerTransaction: Option[ConsumerTransaction] = Some(txn)
    })
    requestChain(txns)
  }

  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): TwitterFuture[Boolean] =
  {
    val transactionService = new Service[Transaction, Boolean] {
      override def apply(request: Transaction): TwitterFuture[Boolean] = {
        getClientTransaction.putTransaction(token, request)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionService)
    requestChain( new Transaction {
      override def producerTransaction: Option[ProducerTransaction] = Some(transaction)
      override def consumerTransaction: Option[ConsumerTransaction] = None
    })
  }

  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): TwitterFuture[Boolean] =
  {
    val transactionService = new Service[Transaction, Boolean] {
      override def apply(request: Transaction): TwitterFuture[Boolean] = {
        getClientTransaction.putTransaction(token, request)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionService)
    requestChain( new Transaction {
      override def producerTransaction: Option[ProducerTransaction] = None
      override def consumerTransaction: Option[ConsumerTransaction] = Some(transaction)
    })
  }

  def putTransactionData(producerTransaction: ProducerTransaction, data: Seq[Seq[Byte]]): TwitterFuture[Boolean] = {
    val transactionDataService = new Service[(ProducerTransaction, Seq[Seq[Byte]]), Boolean] {
      override def apply(request: (ProducerTransaction, Seq[Seq[Byte]])): TwitterFuture[Boolean] = {
        val (txn, dataBinary) = request
        val data = dataBinary map (data => java.nio.ByteBuffer.wrap(data.toArray))
        getClientTransaction.putTransactionData(token,txn.stream,txn.partition,txn.transactionID, data)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionDataService)
    requestChain((producerTransaction, data))
  }

  def putTransactionData(consumerTransaction: ConsumerTransaction, data: Seq[Seq[Byte]]): TwitterFuture[Boolean] = {
    val transactionDataService = new Service[(ConsumerTransaction, Seq[Seq[Byte]]), Boolean] {
      override def apply(request: (ConsumerTransaction, Seq[Seq[Byte]])): TwitterFuture[Boolean] = {
        val (txn, dataBinary) = request
        val data = dataBinary map (data => java.nio.ByteBuffer.wrap(data.toArray))
        getClientTransaction.putTransactionData(token,txn.stream,txn.partition,txn.transactionID, data)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionDataService)
    requestChain((consumerTransaction, data))
  }



  def getTransactionData(consumerTransaction: ConsumerTransaction, from: Int, to: Int): TwitterFuture[Seq[Array[Byte]]] = {
    require(from >=0 && to >=0)
    val transactionDataService = new Service[ConsumerTransaction, Seq[Array[Byte]]] {
      override def apply(request: ConsumerTransaction): TwitterFuture[Seq[Array[Byte]]] = {
        val future = getClientTransaction.getTransactionData(token, request.stream, request.partition, request.transactionID, from, to)
        future map(data => data map {datum =>
          val sizeOfSlicedData = datum.limit() - datum.position()
          val bytes = new Array[Byte](sizeOfSlicedData)
          datum.get(bytes)
          bytes
        })
      }
    }
    val requestChain = retryFilterToken.andThen(transactionDataService)
    requestChain(consumerTransaction)
  }

  def setConsumerState(consumerTransaction: ConsumerTransaction): TwitterFuture[Boolean] = {
    val consumerService = new Service[ConsumerTransaction, Boolean] {
      override def apply(request: ConsumerTransaction): TwitterFuture[Boolean] = {
        getClientTransaction.setConsumerState(token, request.name, request.stream, request.partition, request.transactionID)
      }
    }
    val requestChain = retryFilterToken.andThen(consumerService)
    requestChain(consumerTransaction)
  }

  def getConsumerState(consumerTransaction: (String,String,Int)): TwitterFuture[Long] = {
    val consumerService = new Service[(String,String,Int), Long] {
      override def apply(request: (String,String,Int)): TwitterFuture[Long] = {
        getClientTransaction.getConsumerState(token, request._1, request._2, request._3)
      }
    }
    val requestChain = retryFilterToken.andThen(consumerService)
    requestChain(consumerTransaction)
  }
}

object TransactionZooKeeperClient extends App {
  import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
  val config = new ConfigClient("clientProperties.properties")
  val client = new TransactionZooKeeperClient(config)
  println(Await.result(client.putStream("1",20, None)))

  val rand = scala.util.Random

    val producerTransactions = (0 to 100).map(_ => new ProducerTransaction {
        override val transactionID: Long = rand.nextLong()

        override val state: TransactionStates = TransactionStates.Opened

        override val stream: String = rand.nextInt(1000).toString

        override val timestamp: Long = Time.epoch.inNanoseconds

        override val quantity: Int = -1

        override val partition: Int = rand.nextInt(10000)

        override def tll: Long = Time.epoch.inNanoseconds
      })

      val consumerTransactions = (0 to 100).map(_ => new ConsumerTransaction {
        override def transactionID: Long = scala.util.Random.nextLong()

        override def name: String = rand.nextInt(1000).toString

        override def stream: String = rand.nextInt(1000).toString

        override def partition: Int = rand.nextInt(10000)
      })

  println(Await.result(client.putTransactions(producerTransactions, consumerTransactions)))
}

