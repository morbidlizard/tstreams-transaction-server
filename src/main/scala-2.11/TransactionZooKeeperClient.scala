import authService.AuthClient
import com.twitter.finagle.{Service, ServiceTimeoutException}
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service.{Backoff, RetryExceptionsFilter, RetryPolicy}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Throw, Time, Try, Future => TwitterFuture}
import com.twitter.conversions.time._
import filter.Filter
import org.apache.curator.retry.ExponentialBackoffRetry
import configProperties.ClientConfig
import transactionService.client.TransactionClient
import zooKeeper.ZKLeaderClient
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Transaction}


class TransactionZooKeeperClient {
  import ClientConfig._

  private val logger = Logger.get(this.getClass)
  private val zKLeaderClient = new ZKLeaderClient(zkEndpoints, zkTimeoutSession, zkTimeoutConnection,
    new ExponentialBackoffRetry(zkTimeoutBetweenRetries, zkRetriesMax), zkPrefix)
  zKLeaderClient.start

  private val clientAuth = new AuthClient(authAddress, authTimeoutConnection, authTimeoutExponentialBetweenRetries)
  @volatile private var token = Await.result(clientAuth.authenticate(login, password))
  private val retryConditionToken: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(error) => error match {
      case e: ServiceTimeoutException => true
      case e: com.twitter.finagle.ChannelWriteException => true
      case e =>
        val messageToParse = e.getMessage
        Logger.get().log(Level.ERROR, messageToParse)
        if (messageToParse.contains(exception.Throwables.tokenInvalidExceptionMessage)) {
          token = Await.result(clientAuth.authenticate(login, password))
          true
        } else false
    }
    case _ => false
  }
  private val retryPolicyToken = RetryPolicy.backoff(Backoff.equalJittered(300.milliseconds, 10.seconds))(retryConditionToken)
  private def retryFilterToken[Req, Rep] = new RetryExceptionsFilter[Req, Rep](retryPolicyToken, HighResTimer.Default)

  private val AddressToTransactionServiceServer = new scala.collection.concurrent.TrieMap[String, TransactionClient]()
  private def getClientTransaction = {
    val master = zKLeaderClient.master.get
    AddressToTransactionServiceServer.putIfAbsent(master, new TransactionClient(master))
    AddressToTransactionServiceServer(master)
  }

  private def getMasterFilter[Req, Rep] = Filter
    .retryGetMaster[Req, Rep](authTimeoutConnection, authTimeoutExponentialBetweenRetries)
  private val zkService = getMasterFilter andThen new Service[Unit, TransactionClient] {
    override def apply(request: Unit): TwitterFuture[TransactionClient] = {
      TwitterFuture(getClientTransaction)
    }
  }


  private case class Stream(override val name: String, override val partitions: Int, override val description:Option[String], override val ttl: Int)
    extends transactionService.rpc.Stream

  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Int): TwitterFuture[Boolean] = {
    val streamService = new Service[(TransactionClient,Stream), Boolean] {
      override def apply(request: (TransactionClient,Stream)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.putStream(token, stream.name, stream.partitions, stream.description, ttl)}
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, Stream(stream,partitions,description, ttl)))
  }

  def putStream(stream: Stream): TwitterFuture[Boolean] = {
    val streamService = new Service[(TransactionClient,Stream), Boolean] {
      override def apply(request: (TransactionClient,Stream)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.putStream(token, stream.name, stream.partitions, stream.description, stream.ttl)}
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, stream))
  }

  def delStream(stream: String): TwitterFuture[Boolean] = {
    val streamService = new Service[(TransactionClient, String), Boolean] {
      override def apply(request: (TransactionClient, String)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.delStream(token, stream)
      }
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, stream))
  }

  def delStream(stream: Stream): TwitterFuture[Boolean] = {
    val streamService = new Service[(TransactionClient, Stream), Boolean] {
      override def apply(request: (TransactionClient, Stream)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.delStream(token, stream.name)
      }
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, stream))
  }


  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): TwitterFuture[Boolean] =
  {
    val transactionService = new Service[(TransactionClient, Seq[Transaction]), Boolean] {
      override def apply(request: (TransactionClient, Seq[Transaction])): TwitterFuture[Boolean] = {
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
    val transactionService = new Service[(TransactionClient, Transaction), Boolean] {
      override def apply(request: (TransactionClient, Transaction)): TwitterFuture[Boolean] = {
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
    val transactionService = new Service[(TransactionClient, Transaction), Boolean] {
      override def apply(request: (TransactionClient, Transaction)): TwitterFuture[Boolean] = {
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
    val transactionDataService = new Service[(TransactionClient, ProducerTransaction, Seq[Seq[Byte]]), Boolean] {
      override def apply(request: (TransactionClient, ProducerTransaction, Seq[Seq[Byte]])): TwitterFuture[Boolean] = {
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
    val transactionDataService = new Service[(TransactionClient, ConsumerTransaction, Seq[Seq[Byte]]), Boolean] {
      override def apply(request: (TransactionClient, ConsumerTransaction, Seq[Seq[Byte]])): TwitterFuture[Boolean] = {
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
    val transactionDataService = new Service[(TransactionClient, ConsumerTransaction), Seq[Array[Byte]]] {
      override def apply(request: (TransactionClient, ConsumerTransaction)): TwitterFuture[Seq[Array[Byte]]] = {
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
    val transactionDataService = new Service[(TransactionClient, ProducerTransaction), Seq[Array[Byte]]] {
      override def apply(request: (TransactionClient, ProducerTransaction)): TwitterFuture[Seq[Array[Byte]]] = {
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
    val consumerService = new Service[(TransactionClient, ConsumerTransaction), Boolean] {
      override def apply(request: (TransactionClient, ConsumerTransaction)): TwitterFuture[Boolean] = {
        val (client, txn) = request
        client.setConsumerState(token, txn.name, txn.stream, txn.partition, txn.transactionID)
      }
    }
    val requestChain = retryFilterToken.andThen(consumerService)
    zkService().flatMap(client => requestChain(client, consumerTransaction))
  }

  def getConsumerState(consumerTransaction: (String,String,Int)): TwitterFuture[Long] = {
    val consumerService = new Service[(TransactionClient,String,String,Int), Long] {
      override def apply(request: (TransactionClient,String,String,Int)): TwitterFuture[Long] = {
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
  val client = new TransactionZooKeeperClient
  println(Await.result(client.putStream("1",20, None, 5)))

  val rand = scala.util.Random

    val producerTransactions = (0 to 100000).map(_ => new ProducerTransaction {
        override val transactionID: Long = rand.nextLong()

        override val state: TransactionStates = TransactionStates.Opened

        override val stream: String = "1"

        override val timestamp: Long = Time.epoch.inNanoseconds

        override val quantity: Int = -1

        override val partition: Int = rand.nextInt(10000)
      })

      val consumerTransactions = (0 to 100000).map(_ => new ConsumerTransaction {
        override def transactionID: Long = scala.util.Random.nextLong()

        override def name: String = rand.nextInt(10000).toString

        override def stream: String = "1"

        override def partition: Int = rand.nextInt(10000)
      })


  println(Await.result(client.putTransactions(producerTransactions, Seq())))

  val data = (0 to 1000000) map (_ => rand.nextString(10).getBytes().toSeq)

  println(Await.result(client.putTransactionData(producerTransactions.head, data)))
}

