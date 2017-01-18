import java.io.File
import java.util.concurrent.atomic.LongAdder

import org.apache.commons.io.FileUtils
import netty.client.Client
import netty.server.Server
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class Test extends FlatSpec with Matchers with BeforeAndAfterEach {
  var client: Client = _
  var transactionServer: Server = _

  private val configServer = new configProperties.ServerConfig(new configProperties.ConfigFile("src/main/resources/serverProperties.properties"))
  private val configClient = new configProperties.ClientConfig(new configProperties.ConfigFile("src/main/resources/clientProperties.properties"))
  def startTransactionServer() = {
    new Thread(new Runnable {
      override def run(): Unit = {
        transactionServer = new netty.server.Server()
        transactionServer.start()
      }
    }).start()
  }

  override def beforeEach(): Unit = {
    startTransactionServer()
    client = new Client
  }

  override def afterEach() {
    transactionServer.close()
    client.close()
    FileUtils.deleteDirectory(new File(configServer.dbPath + "/" + configServer.dbStreamDirName))
    FileUtils.deleteDirectory(new File(configServer.dbPath + "/" + configServer.dbTransactionDataDirName))
    FileUtils.deleteDirectory(new File(configServer.dbPath + "/" + configServer.dbTransactionMetaDirName))
  }

  implicit object ProducerTransactionSortable extends Ordering[ProducerTransaction] {
    override def compare(x: ProducerTransaction, y: ProducerTransaction): Int = {
      if (x.stream > y.stream) 1
      else if (x.stream < y.stream) -1
      else if (x.partition > y.partition) 1
      else if (x.partition < y.partition) -1
      else if (x.transactionID > y.transactionID) 1
      else if (x.transactionID < y.transactionID) -1
      else 0
    }
  }
  
  private val rand = scala.util.Random
  private def getRandomStream = new transactionService.rpc.Stream {
    override val name: String = rand.nextInt(10000).toString
    override val partitions: Int = rand.nextInt(10000)
    override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
    override val ttl: Int = rand.nextInt(Int.MaxValue)
  }
  private def chooseStreamRandomly(streams: IndexedSeq[transactionService.rpc.Stream]) = streams(rand.nextInt(streams.length))

  private def getRandomProducerTransaction(streamObj: transactionService.rpc.Stream) = new ProducerTransaction {
    override val transactionID: Long = System.nanoTime()
    override val state: TransactionStates = TransactionStates(rand.nextInt(TransactionStates(2).value) + 1)
    override val stream: String = streamObj.name
    override val keepAliveTTL: Long = Long.MaxValue
    override val quantity: Int = -1
    override val partition: Int = streamObj.partitions
  }

  private def getRandomConsumerTransaction(streamObj: transactionService.rpc.Stream) =  new ConsumerTransaction {
    override val transactionID: Long = scala.util.Random.nextLong()
    override val name: String = rand.nextInt(10000).toString
    override val stream: String = streamObj.name
    override val partition: Int = streamObj.partitions
  }


  val secondsWait = 5


    "Client" should "put producer and consumer transactions" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait seconds)

    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val result = client.putTransactions(producerTransactions, consumerTransactions)

    Await.result(result, 5 seconds) shouldBe true
  }

  it should "put stream, then delete this stream, and server shouldn't save producer and consumer transactions on putting them by client" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait seconds)
    Await.result(client.delStream(stream), secondsWait seconds)

    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val result = client.putTransactions(producerTransactions, consumerTransactions)
    assertThrows[exception.Throwables.StreamNotExist] {
      Await.result(result, secondsWait seconds)
    }
  }

  it should "throw an exception when the auth server isn't available for time greater than in config" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait seconds)

    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)

    transactionServer.close()
    assertThrows[exception.Throwables.ServerUnreachableException] {
      Await.result(resultInFuture, configClient.authTimeoutConnection + 1000 milliseconds)
    }
  }

  it should "not throw an exception when the auth server isn't available for time less than in config" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait seconds)

    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)

    Thread.sleep(configClient.authTimeoutConnection*3/5)

    Await.result(resultInFuture, secondsWait seconds) shouldBe true
  }

  it should "put any kind of binary data and get it back" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait seconds)

    val txn = getRandomProducerTransaction(stream)
    Await.result(client.putTransaction(txn), secondsWait seconds)

    val amount = 5000
    val data = Array.fill(amount)(rand.nextString(10).getBytes)

    val resultInFuture = Await.result(client.putTransactionData(txn, data, 0), secondsWait seconds)
    resultInFuture shouldBe true

    val dataFromDatabase = Await.result(client.getTransactionData(txn,0, amount), secondsWait seconds)
    data should contain theSameElementsAs dataFromDatabase
  }

  it should "put transactions and get them back(scanTransactions)" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait seconds)

    val producerTransactions = Array.fill(15)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    Await.result(client.putTransactions(producerTransactions, Seq()), secondsWait seconds)

    val statesAllowed = Array(TransactionStates.Opened,TransactionStates.Checkpointed)
    val (from, to) = (
      producerTransactions.filter(txn => statesAllowed.contains(txn.state)).minBy(_.transactionID).transactionID,
      producerTransactions.filter(txn => statesAllowed.contains(txn.state)).maxBy(_.transactionID).transactionID
      )

    val producerTransactionsByState = producerTransactions.groupBy(_.state)
    val res = Await.result(client.scanTransactions(stream.name, stream.partitions, from, to), secondsWait seconds)

    val txns = producerTransactionsByState(TransactionStates.Opened).sortBy(_.transactionID)

    res should contain theSameElementsAs txns
    res shouldBe sorted
  }

  it should "put consumerTransaction and get it back" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait seconds)

    val consumerTransaction = getRandomConsumerTransaction(stream)

    Await.result(client.setConsumerState(consumerTransaction), secondsWait seconds)

    val consumerState = Await.result(client.getConsumerState(consumerTransaction.name, consumerTransaction.stream, consumerTransaction.partition), secondsWait seconds)

    consumerState shouldBe consumerTransaction.transactionID
  }

  "Server" should "not save producer and consumer transactions, that don't refer to a stream in database they should belong to" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait seconds)

    val streamFake = getRandomStream
    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamFake))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamFake))

    val result = client.putTransactions(producerTransactions, consumerTransactions)
    assertThrows[exception.Throwables.StreamNotExist] {
      Await.result(result, secondsWait seconds)
    }
  }

  it should "not have problems with many clients" in {
    val clietnsNum = 5
    val clients = Array.fill(clietnsNum)(new Client)
    val streams = Array.fill(10000)(getRandomStream)
    Await.result(client.putStream(chooseStreamRandomly(streams)), secondsWait seconds)

    val dataCounter = new java.util.concurrent.ConcurrentHashMap[(String,Int), LongAdder]()
    def addDataLength(stream: String, partition: Int, dataLength: Int): Unit = {
      val valueToAdd = if (dataCounter.containsKey((stream,partition))) dataLength else 0
      dataCounter.computeIfAbsent((stream,partition), new java.util.function.Function[(String,Int), LongAdder]{
        override def apply(t: (String, Int)): LongAdder = new LongAdder()
      }).add(valueToAdd)
    }
    def getDataLength(stream: String, partition: Int) = dataCounter.get((stream,partition)).intValue()


    val res: Future[mutable.ArraySeq[Boolean]] = Future.sequence(clients map { client =>
      val streamFake = getRandomStream
      client.putStream(streamFake).flatMap{_ =>
        val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamFake))
        val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamFake))
        val data = Array.fill(100)(rand.nextInt(10000).toString.getBytes)

        client.putTransactions(producerTransactions, consumerTransactions)

        val (stream, partition) = (producerTransactions.head.stream, producerTransactions.head.partition)
        addDataLength(stream, partition, data.length)
        client.putTransactionData(producerTransactions.head, data, getDataLength(stream, partition))
      }
    })

    all(Await.result(res, secondsWait*clietnsNum seconds)) shouldBe true
  }
}
