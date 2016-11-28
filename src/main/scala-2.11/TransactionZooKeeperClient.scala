import authService.ClientAuth
import com.twitter.finagle.{Service, ServiceTimeoutException}
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service.{Backoff, RetryExceptionsFilter, RetryPolicy}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Throw, Time, Try, Future => TwitterFuture}
import com.twitter.conversions.time._
import filter.Filter
import org.apache.curator.retry.ExponentialBackoffRetry
import resource.ConfigClient
import transactionService.client.ClientTransaction
import zooKeeper.ZKLeaderClient
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Transaction}


class TransactionZooKeeperClient(private val config: ConfigClient) {
  import config._

  private val logger = Logger.get(this.getClass)
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

  private def getMasterFilter[Req, Rep] = Filter
    .retryGetMaster[Req, Rep](authTimeoutConnection, authTimeoutExponentialBetweenRetries, logger)
  private val zkService = getMasterFilter andThen new Service[Unit, ClientTransaction] {
    override def apply(request: Unit): TwitterFuture[ClientTransaction] = {
      TwitterFuture(getClientTransaction)
    }
  }


  private case class Stream(override val name: String, override val partitions: Int, override val description:Option[String])
    extends transactionService.rpc.Stream

  def putStream(stream: String, partitions: Int, description: Option[String]): TwitterFuture[Boolean] = {
    val streamService = new Service[(ClientTransaction,Stream), Boolean] {
      override def apply(request: (ClientTransaction,Stream)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.putStream(token, stream.name, stream.partitions, stream.description)}
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, Stream(stream,partitions,description)))
  }

  def putStream(stream: Stream): TwitterFuture[Boolean] = {
    val streamService = new Service[(ClientTransaction,Stream), Boolean] {
      override def apply(request: (ClientTransaction,Stream)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.putStream(token, stream.name, stream.partitions, stream.description)}
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, stream))
  }

  def delStream(stream: String): TwitterFuture[Boolean] = {
    val streamService = new Service[(ClientTransaction, String), Boolean] {
      override def apply(request: (ClientTransaction, String)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.delStream(token, stream)
      }
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, stream))
  }

  def delStream(stream: Stream): TwitterFuture[Boolean] = {
    val streamService = new Service[(ClientTransaction, Stream), Boolean] {
      override def apply(request: (ClientTransaction, Stream)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.delStream(token, stream.name)
      }
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, stream))
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
    val transactionService = new Service[(ClientTransaction, Seq[Transaction]), Boolean] {
      override def apply(request: (ClientTransaction, Seq[Transaction])): TwitterFuture[Boolean] = {
        val (client,txns) = request
        client.putTransactions(token, txns)
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
    zkService().flatMap(client => requestChain(client, txns))
  }

  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): TwitterFuture[Boolean] =
  {
    val transactionService = new Service[(ClientTransaction, Transaction), Boolean] {
      override def apply(request: (ClientTransaction, Transaction)): TwitterFuture[Boolean] = {
        val (client, txn) = request
        client.putTransaction(token, txn)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionService)
    zkService().flatMap(client => requestChain(client, new Transaction {
      override def producerTransaction: Option[ProducerTransaction] = Some(transaction)
      override def consumerTransaction: Option[ConsumerTransaction] = None
    }))
  }

  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): TwitterFuture[Boolean] =
  {
    val transactionService = new Service[(ClientTransaction, Transaction), Boolean] {
      override def apply(request: (ClientTransaction, Transaction)): TwitterFuture[Boolean] = {
        val (client, txn) = request
        client.putTransaction(token, txn)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionService)
    zkService().flatMap(client => requestChain(client, new Transaction {
      override def producerTransaction: Option[ProducerTransaction] = None
      override def consumerTransaction: Option[ConsumerTransaction] = Some(transaction)
    }))
  }

  //TODO add from: Int to signature
  def putTransactionData(producerTransaction: ProducerTransaction, data: Seq[Seq[Byte]]): TwitterFuture[Boolean] = {
    val transactionDataService = new Service[(ClientTransaction, ProducerTransaction, Seq[Seq[Byte]]), Boolean] {
      override def apply(request: (ClientTransaction, ProducerTransaction, Seq[Seq[Byte]])): TwitterFuture[Boolean] = {
        val (client,txn, dataBinary) = request
        val data = dataBinary map (data => java.nio.ByteBuffer.wrap(data.toArray))
        client.putTransactionData(token,txn.stream,txn.partition,txn.transactionID, data)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionDataService)
    zkService().flatMap(client => requestChain((client, producerTransaction, data)))
  }

  //TODO add from: Int to signature
  def putTransactionData(consumerTransaction: ConsumerTransaction, data: Seq[Seq[Byte]]): TwitterFuture[Boolean] = {
    val transactionDataService = new Service[(ClientTransaction, ConsumerTransaction, Seq[Seq[Byte]]), Boolean] {
      override def apply(request: (ClientTransaction, ConsumerTransaction, Seq[Seq[Byte]])): TwitterFuture[Boolean] = {
        val (client,txn, binaryData) = request
        val data = binaryData map (data => java.nio.ByteBuffer.wrap(data.toArray))
        client.putTransactionData(token,txn.stream,txn.partition,txn.transactionID, data)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionDataService)
    zkService().flatMap(client => requestChain((client, consumerTransaction, data)))
  }



  def getTransactionData(consumerTransaction: ConsumerTransaction, from: Int, to: Int): TwitterFuture[Seq[Array[Byte]]] = {
    require(from >=0 && to >=0)
    val transactionDataService = new Service[(ClientTransaction, ConsumerTransaction), Seq[Array[Byte]]] {
      override def apply(request: (ClientTransaction, ConsumerTransaction)): TwitterFuture[Seq[Array[Byte]]] = {
        val (client, txn) = request
        val future = client.getTransactionData(token, txn.stream, txn.partition, txn.transactionID, from, to)
        future map(data => data map {datum =>
          val sizeOfSlicedData = datum.limit() - datum.position()
          val bytes = new Array[Byte](sizeOfSlicedData)
          datum.get(bytes)
          bytes
        })
      }
    }
    val requestChain = retryFilterToken.andThen(transactionDataService)
    zkService().flatMap(client => requestChain(client, consumerTransaction))
  }

  def getTransactionData(producerTransaction: ProducerTransaction, from: Int, to: Int): TwitterFuture[Seq[Array[Byte]]] = {
    require(from >=0 && to >=0)
    val transactionDataService = new Service[(ClientTransaction, ProducerTransaction), Seq[Array[Byte]]] {
      override def apply(request: (ClientTransaction, ProducerTransaction)): TwitterFuture[Seq[Array[Byte]]] = {
        val (client, txn) = request
        val future = client.getTransactionData(token, txn.stream, txn.partition, txn.transactionID, from, to)
        future map(data => data map {datum =>
          val sizeOfSlicedData = datum.limit() - datum.position()
          val bytes = new Array[Byte](sizeOfSlicedData)
          datum.get(bytes)
          bytes
        })
      }
    }
    val requestChain = retryFilterToken.andThen(transactionDataService)
    zkService().flatMap(client => requestChain(client, producerTransaction))
  }

  def setConsumerState(consumerTransaction: ConsumerTransaction): TwitterFuture[Boolean] = {
    val consumerService = new Service[(ClientTransaction, ConsumerTransaction), Boolean] {
      override def apply(request: (ClientTransaction, ConsumerTransaction)): TwitterFuture[Boolean] = {
        val (client, txn) = request
        client.setConsumerState(token, txn.name, txn.stream, txn.partition, txn.transactionID)
      }
    }
    val requestChain = retryFilterToken.andThen(consumerService)
    zkService().flatMap(client => requestChain(client, consumerTransaction))
  }

  def getConsumerState(consumerTransaction: (String,String,Int)): TwitterFuture[Long] = {
    val consumerService = new Service[(ClientTransaction,String,String,Int), Long] {
      override def apply(request: (ClientTransaction,String,String,Int)): TwitterFuture[Long] = {
        val (client, name, stream, partition) = request
        client.getConsumerState(token,name, stream, partition)
      }
    }
    val requestChain = retryFilterToken.andThen(consumerService)
    val (name,stream,partition) = consumerTransaction
    zkService().flatMap(client => requestChain((client, name ,stream, partition)))
  }
}

object TransactionZooKeeperClient extends App {
  import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
  val config = new ConfigClient("clientProperties.properties")
  val client = new TransactionZooKeeperClient(config)
  println(Await.result(client.putStream("1",20, None)))

  val rand = scala.util.Random

    val producerTransactions = (0 to 100000).map(_ => new ProducerTransaction {
        override val transactionID: Long = rand.nextLong()

        override val state: TransactionStates = TransactionStates.Opened

        override val stream: String = rand.nextInt(10000).toString

        override val timestamp: Long = Time.epoch.inNanoseconds

        override val quantity: Int = -1

        override val partition: Int = rand.nextInt(10000)

        override def tll: Long = Time.epoch.inNanoseconds
      })

      val consumerTransactions = (0 to 100000).map(_ => new ConsumerTransaction {
        override def transactionID: Long = scala.util.Random.nextLong()

        override def name: String = rand.nextInt(10000).toString

        override def stream: String = rand.nextInt(10000).toString

        override def partition: Int = rand.nextInt(10000)
      })


  println(Await.result(client.putTransactions(producerTransactions, Seq())))

  val data = (0 to 1000000) map (_ => rand.nextString(10).getBytes().toSeq)

  println(Await.result(client.putTransactionData(producerTransactions.head, data)))
}

