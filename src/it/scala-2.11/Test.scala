import java.io.File
import java.util.concurrent.atomic.LongAdder

import authService.AuthServer
import com.twitter.util.{Await, Closable, Time}
import configProperties.DB
import org.apache.commons.io.FileUtils
import com.twitter.util.{Future => TwitterFuture}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
import transactionZookeeperService.{TransactionZooKeeperClient, TransactionZooKeeperServer}

class Test extends FlatSpec with Matchers with BeforeAndAfterEach {
  val client: TransactionZooKeeperClient = new TransactionZooKeeperClient
  var transactionServer: TransactionZooKeeperServer = _
  var authServer: AuthServer = _

  override def beforeEach(): Unit = {
    transactionServer = new TransactionZooKeeperServer
    authServer = new AuthServer

    transactionServer.start()
    authServer.start()
  }

  override def afterEach() {
    Await.result(Closable.all(transactionServer, authServer).close())
    FileUtils.deleteDirectory(new File(DB.PathToDatabases + "/" + DB.StreamDirName))
    FileUtils.deleteDirectory(new File(DB.PathToDatabases + "/" + DB.TransactionDataDirName))
    FileUtils.deleteDirectory(new File(DB.PathToDatabases + "/" + DB.TransactionMetaDirName))
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
    override val transactionID: Long = rand.nextLong()
    override val state: TransactionStates = TransactionStates(rand.nextInt(TransactionStates.list.length) + 1)
    override val stream: String = streamObj.name
    override val timestamp: Long = Time.epoch.inNanoseconds
    override val quantity: Int = -1
    override val partition: Int = rand.nextInt(10000)
  }

  private def getRandomConsumerTransaction(streamObj: transactionService.rpc.Stream) =  new ConsumerTransaction {
    override def transactionID: Long = scala.util.Random.nextLong()
    override def name: String = rand.nextInt(10000).toString
    override def stream: String = streamObj.name
    override def partition: Int = rand.nextInt(10000)
  }

//  val dataCounter = new java.util.concurrent.ConcurrentHashMap[(String,Int), LongAdder]()
//  def addDataLength(stream: String, partition: Int, dataLength: Int): Unit = {
//    val valueToAdd = if (dataCounter.containsKey((stream,partition))) dataLength else 0
//    dataCounter.computeIfAbsent((stream,partition), new java.util.function.Function[(String,Int), LongAdder]{
//      override def apply(t: (String, Int)): LongAdder = new LongAdder()
//    }).add(valueToAdd)
//  }
//  def getDataLength(stream: String, partition: Int) = dataCounter.get((stream,partition)).intValue()




  //  "TransactionZooKeeperClient" should "put producer and consumer transactions" in {
//    val stream = getRandomStream
//    Await.result(client.putStream(stream))
//
//    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
//    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))
//
//    val result = client.putTransactions(producerTransactions, consumerTransactions)
//
//    Await.result(result) shouldBe true
//  }

//  it should "put stream, then delete this stream, and server shouldn't save producer and consumer transactions on putting them by client" in {
//    val stream = getRandomStream
//    Await.result(client.putStream(stream))
//    Await.result(client.delStream(stream))
//
//    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
//    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))
//
//    val result = client.putTransactions(producerTransactions, consumerTransactions)
//    assertThrows[org.apache.thrift.TApplicationException] {
//      Await.result(result)
//    }
//  }
//
//  it should "not throw an exception when the auth server isn't available for time less than in config" in {
//    val stream = getRandomStream
//    Await.result(client.putStream(stream))
//
//    authServer.close()
//
//    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
//    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))
//
//
//    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)
//
//
//    assertThrows[org.apache.thrift.TApplicationException] {
//      Await.result(resultInFuture)
//    }
//  }

  it should "put any kind of binary data and get it back" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream))

    val txn = getRandomProducerTransaction(stream)
    Await.result(client.putTransaction(txn))

    val data = Array.fill(50)(rand.nextString(10).getBytes)


    val resultInFuture = client.putTransactionData(txn, data, 0)

    Await.result(resultInFuture) shouldBe true

    val dataFromDatabase = Await.result(client.getTransactionData(txn,0, 50))

    data should contain theSameElementsAs dataFromDatabase
  }

//  "TransactionZooKeeperServer" should "not save producer and consumer transactions, that don't refer to a stream in database they should belong to" in {
//    val stream = getRandomStream
//    Await.result(client.putStream(stream))
//
//    val streamFake = getRandomStream
//    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamFake))
//    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamFake))
//
//    val result = client.putTransactions(producerTransactions, consumerTransactions)
//    assertThrows[org.apache.thrift.TApplicationException] {
//      Await.result(result)
//    }
//  }
//
//  "TransactionZooKeeperServer" should "not have problems with many clients" in {
//    val clients = Array.fill(3)(new TransactionZooKeeperClient)
//    val streams = Array.fill(10000)(getRandomStream)
//    Await.result(client.putStream(chooseStreamRandomly(streams)))
//
//
//    val res = TwitterFuture.collect(clients map {client =>
//      val streamFake = getRandomStream
//      val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamFake))
//      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamFake))
//
//      client.putTransactions(producerTransactions,consumerTransactions)
//    })
//
//    assertThrows[org.apache.thrift.TApplicationException] {
//      Await.result(res)
//    }
//  }
  
}
