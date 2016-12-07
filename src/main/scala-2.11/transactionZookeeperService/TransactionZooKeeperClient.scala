package transactionZookeeperService


import java.util.concurrent.Executors

import authService.AuthClient
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service.{Backoff, RetryExceptionsFilter, RetryPolicy}
import com.twitter.finagle.Service
import com.twitter.logging.{Level, Logger}
import com.twitter.util.FuturePool
import com.twitter.conversions.time._
import com.twitter.util.{Await, Future => TwitterFuture, Throw, Time, Try}
import filter.Filter
import org.apache.curator.retry.RetryNTimes
import configProperties.ClientConfig
import transactionService.client.TransactionClient
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Transaction}
import zooKeeper.ZKLeaderClient


class TransactionZooKeeperClient {
  import ClientConfig._

  private val zKLeaderClient = new ZKLeaderClient(zkEndpoints, zkTimeoutSession, zkTimeoutConnection,
    new RetryNTimes(zkRetriesMax, zkTimeoutBetweenRetries), zkPrefix)
  zKLeaderClient.start()

  private val clientAuth = new AuthClient(authAddress, authTimeoutConnection, authTimeoutBetweenRetries)
  @volatile private var token: String = _
  private val retryConditionToken: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(error) => error match {
      case e =>
        val messageToParse = e.getMessage
        Logger.get().log(Level.ERROR, messageToParse)
        if (messageToParse.contains(exception.Throwables.tokenInvalidExceptionMessage)) {
          clientAuth.authenticate(login, password).map(newToken => token = newToken)
          true
        } else false
    }
    case _ => false
  }

  private val retryPolicyToken = RetryPolicy.backoff(Backoff.const(authTokenTimeoutBetweenRetries.milliseconds)
    .take(authTokenTimeoutConnection/authTokenTimeoutBetweenRetries))(retryConditionToken)

  private def retryFilterToken[Req, Rep] = new RetryExceptionsFilter[Req, Rep](retryPolicyToken, shared.SharedTimer.highResTimer)

  private val AddressToTransactionServiceServer = new java.util.concurrent.ConcurrentHashMap[String, TransactionClient]()
  private def getClientTransaction = {
    val master = zKLeaderClient.master.get
    if (AddressToTransactionServiceServer.containsKey(master))
      AddressToTransactionServiceServer.get(master)
    else {
      val newClient = new TransactionClient(master)
      if (AddressToTransactionServiceServer.putIfAbsent(master, newClient) == null)
        newClient else AddressToTransactionServiceServer.get(master)
    }
  }

  private def getMasterFilter[Req, Rep] = Filter
    .filter[Req, Rep](zkTimeoutConnection, zkTimeoutBetweenRetries, Filter.retryConditionToGetMaster)

  private val zkService = getMasterFilter andThen new Service[Unit, TransactionClient] {
    override def apply(request: Unit): TwitterFuture[TransactionClient] = {
      TwitterFuture(getClientTransaction)
    }
  }


  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Int): TwitterFuture[Boolean] = {
    val streamService = new Service[(TransactionClient, transactionService.rpc.Stream), Boolean] {
      override def apply(request: (TransactionClient, transactionService.rpc.Stream)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.putStream(token, stream.name, stream.partitions, stream.description, ttl)
      }
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, transactionService.rpc.Stream(stream, partitions, description, ttl)))
  }


  def putStream(stream: transactionService.rpc.Stream): TwitterFuture[Boolean] = {
    val streamService = new Service[(TransactionClient,transactionService.rpc.Stream), Boolean] {
      override def apply(request: (TransactionClient,transactionService.rpc.Stream)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.putStream(token, stream.name, stream.partitions, stream.description, stream.ttl)
      }
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

  def delStream(stream: transactionService.rpc.Stream): TwitterFuture[Boolean] = {
    val streamService = new Service[(TransactionClient, transactionService.rpc.Stream), Boolean] {
      override def apply(request: (TransactionClient, transactionService.rpc.Stream)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.delStream(token, stream.name)
      }
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, stream))
  }

  def doesStreamExist(stream: String) = {
    val streamService = new Service[(TransactionClient, String), Boolean] {
      override def apply(request: (TransactionClient, String)): TwitterFuture[Boolean] = {
        val (client, stream) = request
        client.doesStreamExist(token, stream)
      }
    }
    val requestChain = retryFilterToken.andThen(streamService)
    zkService().flatMap(client => requestChain(client, stream))
  }

  private val executor = Executors.newSingleThreadExecutor

  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): TwitterFuture[Boolean] = {
    val transactionService = new Service[(TransactionClient, Seq[Transaction]), Boolean] {
      override def apply(request: (TransactionClient, Seq[Transaction])): TwitterFuture[Boolean] = {
        val (client, txns) = request
        FuturePool.interruptible(executor)(client.putTransactions(token, txns)).flatten
      }
    }
    val requestChain = retryFilterToken.andThen(transactionService)
    val txns =
      producerTransactions.map(txn => Transaction(Some(txn), None)) ++
      consumerTransactions.map(txn => Transaction(None, Some(txn)))

    zkService().flatMap(client => requestChain(client, txns))
  }

  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): TwitterFuture[Boolean] = {
    val transactionService = new Service[(TransactionClient, Transaction), Boolean] {
      override def apply(request: (TransactionClient, Transaction)): TwitterFuture[Boolean] = {
        val (client, txn) = request
        FuturePool.interruptible(executor)(client.putTransaction(token, txn)).flatten
      }
    }
    val requestChain = retryFilterToken.andThen(transactionService)
    zkService().flatMap(client => requestChain(client, Transaction(Some(transaction), None)))
  }

  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): TwitterFuture[Boolean] = {
    val transactionService = new Service[(TransactionClient, Transaction), Boolean] {
      override def apply(request: (TransactionClient, Transaction)): TwitterFuture[Boolean] = {
        val (client, txn) = request
        FuturePool.interruptible(executor)(client.putTransaction(token, txn)).flatten
      }
    }
    val requestChain = retryFilterToken.andThen(transactionService)
    zkService().flatMap(client => requestChain(client, Transaction(None, Some(transaction))))
  }

  def scanTransactions(stream: String, partition: Int): TwitterFuture[Seq[Transaction]] = {
    val transactionService = new Service[(TransactionClient, String, Int), Seq[Transaction]] {
      override def apply(request: (TransactionClient, String, Int)): TwitterFuture[Seq[Transaction]] = {
        val (client, stream, partition) = request
        client.scanTransactions(token, stream, partition)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionService)
    zkService().flatMap(client => requestChain((client, stream, partition)))
  }


  //TODO add from: Int to signature

  def putTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, data: Seq[Array[Byte]]): TwitterFuture[Boolean] = {
    val transactionDataService = new Service[(TransactionClient, ProducerTransaction, Seq[Array[Byte]]), Boolean] {
      override def apply(request: (TransactionClient, ProducerTransaction, Seq[Array[Byte]])): TwitterFuture[Boolean] = {
        val (client,txn, dataBinary) = request
        val data = dataBinary map java.nio.ByteBuffer.wrap
        client.putTransactionData(token,txn.stream,txn.partition,txn.transactionID, data)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionDataService)
    zkService().flatMap(client => requestChain((client, producerTransaction, data)))
  }

  //TODO add from: Int to signature
  def putTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, data: Seq[Array[Byte]]): TwitterFuture[Boolean] = {
    val transactionDataService = new Service[(TransactionClient, ConsumerTransaction, Seq[Array[Byte]]), Boolean] {
      override def apply(request: (TransactionClient, ConsumerTransaction, Seq[Array[Byte]])): TwitterFuture[Boolean] = {
        val (client,txn, binaryData) = request
        val data = binaryData map java.nio.ByteBuffer.wrap
        client.putTransactionData(token,txn.stream,txn.partition,txn.transactionID, data)
      }
    }
    val requestChain = retryFilterToken.andThen(transactionDataService)
    zkService().flatMap(client => requestChain((client, consumerTransaction, data)))
  }



  def getTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, from: Int, to: Int): TwitterFuture[Seq[Array[Byte]]] = {
    require(from >=0 && to >=0)
    val transactionDataService = new Service[(TransactionClient, ConsumerTransaction), Seq[Array[Byte]]] {
      override def apply(request: (TransactionClient, ConsumerTransaction)): TwitterFuture[Seq[Array[Byte]]] = {
        val (client, txn) = request
        val future = client.getTransactionData(token, txn.stream, txn.partition, txn.transactionID, from, to)
        future map (data => data map { datum =>
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
    require(from >= 0 && to >= 0)
    val transactionDataService = new Service[(TransactionClient, ProducerTransaction), Seq[Array[Byte]]] {
      override def apply(request: (TransactionClient, ProducerTransaction)): TwitterFuture[Seq[Array[Byte]]] = {
        val (client, txn) = request
        val future = client.getTransactionData(token, txn.stream, txn.partition, txn.transactionID, from, to)
        future map (data => data map { datum =>
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

  def getConsumerState(consumerTransaction: (String, String, Int)): TwitterFuture[Long] = {
    val consumerService = new Service[(TransactionClient, String, String, Int), Long] {
      override def apply(request: (TransactionClient, String, String, Int)): TwitterFuture[Long] = {
        val (client, name, stream, partition) = request
        client.getConsumerState(token, name, stream, partition)
      }
    }
    val requestChain = retryFilterToken.andThen(consumerService)
    val (name, stream, partition) = consumerTransaction
    zkService().flatMap(client => requestChain((client, name, stream, partition)))
  }
}

object TransactionZooKeeperClient extends App {

  import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}

  val client = new TransactionZooKeeperClient
  println(Await.result(client.putStream("1", 20, None, 5)))

  val rand = scala.util.Random

  val producerTransactions = (0 to 100000).map(_ => new ProducerTransaction {
    override val transactionID: Long = rand.nextLong()

    override val state: TransactionStates = TransactionStates.Opened

    override val stream: String = "1"

    override val timestamp: Long = Time.epoch.inNanoseconds

    override val quantity: Int = -1

    override val partition: Int = rand.nextInt(10000)
  })

  val consumerTransactions = (0 to 10).map(_ => new ConsumerTransaction {
    override def transactionID: Long = scala.util.Random.nextLong()

    override def name: String = rand.nextInt(10000).toString

    override def stream: String = "1"

    override def partition: Int = rand.nextInt(10000)
  })


  println(Await.result(client.putTransactions(producerTransactions, Seq())))

  val data = (0 to 100000) map (_ => rand.nextString(10).getBytes())

  Await.result(client.putTransactionData(producerTransactions.head, data))

  val timeBefore = System.currentTimeMillis()

  println(Await.result(client.getTransactionData(producerTransactions.head, 0, 10000)))

  val timeAfter = System.currentTimeMillis()

  println(timeAfter-timeBefore)
}

