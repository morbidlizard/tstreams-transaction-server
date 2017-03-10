package it

import java.io.File

import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}

import scala.concurrent.duration._
import scala.concurrent.Await

class ServerClientInterconnectionLifecycleTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  var zkTestServer: TestingServer = _
  var client: Client = _
  var transactionServer: Server = _

  val clientsNum = 2

  private val serverBuilder = new ServerBuilder()
  private val clientBuilder = new ClientBuilder()
  private val storageOptions = serverBuilder.getStorageOptions()
  private val authOptions = clientBuilder.getAuthOptions()

  def startTransactionServer() = new Thread(() => {
    transactionServer = serverBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()
    transactionServer.start()
  }).start()


  override def beforeEach(): Unit = {
    zkTestServer = new TestingServer(true)
    startTransactionServer()
    client = clientBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()
  }

  override def afterEach() {
    client.shutdown()
    transactionServer.shutdown()
    zkTestServer.close()
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.streamDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
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
    override val ttl: Long = Long.MaxValue
  }

  private def chooseStreamRandomly(streams: IndexedSeq[transactionService.rpc.Stream]) = streams(rand.nextInt(streams.length))

  private def getRandomProducerTransaction(streamObj: transactionService.rpc.Stream, ttlTxn: Long = Long.MaxValue) = new ProducerTransaction {
    override val transactionID: Long = System.nanoTime()
    override val state: TransactionStates = TransactionStates(rand.nextInt(TransactionStates.list.length) + 1)
    override val stream: String = streamObj.name
    override val ttl: Long = ttlTxn
    override val quantity: Int = -1
    override val partition: Int = streamObj.partitions
  }

  private def getRandomConsumerTransaction(streamObj: transactionService.rpc.Stream) = new ConsumerTransaction {
    override val transactionID: Long = scala.util.Random.nextLong()
    override val name: String = rand.nextInt(10000).toString
    override val stream: String = streamObj.name
    override val partition: Int = streamObj.partitions
  }


  val secondsWait = 5


//  it should "put stream, then delete this stream, and put it again and return correct result" in {
//    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
//    val streamAfterDelete = transactionService.rpc.Stream("stream_test", 10, Some("Previous one was deleted"), 538L)
//
//    Await.result(client.putStream(stream), secondsWait.seconds)
//    Await.result(client.delStream(stream), secondsWait.seconds)
//    Await.result(client.putStream(streamAfterDelete), secondsWait.seconds)
//
//    val retrievedStream = Await.result(client.getStream(streamAfterDelete.name), secondsWait.seconds)
//
//    streamAfterDelete shouldBe retrievedStream
//  }

  private def getProducerTransactionFromServer(producerTransaction: ProducerTransaction) = {
    Await.result(client.scanTransactions(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID,  producerTransaction.transactionID), secondsWait.seconds).head
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Checkpointed. Should return Checkpointed Transaction" in {
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(client.putStream(stream), secondsWait.seconds)

    val openedTTL = 7
    val producerTransaction = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    Await.result(client.putTransaction(producerTransaction), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTransaction) shouldBe producerTransaction

    val checkpointedTTL = 6
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    Await.result(client.putTransaction(producerTransactionCheckpointed), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTransactionCheckpointed) shouldBe producerTransactionCheckpointed
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Checkpointed. Should return Invalid Transaction(due to expiration)" in {
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(client.putStream(stream), secondsWait.seconds)

    val openedTTL = 4
    val producerTransaction = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    Await.result(client.putTransaction(producerTransaction), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTransaction) shouldBe producerTransaction

    val checkpointedTTL = 2
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    Await.result(client.putTransaction(producerTransactionCheckpointed), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTransactionCheckpointed) shouldBe transactionService.rpc.ProducerTransaction(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, TransactionStates.Invalid, -1, 0)
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Updated->Updated->Checkpointed. Should return Checkpointed Transaction" in {
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(client.putStream(stream), secondsWait.seconds)

    val openedTTL = 7
    val producerTxnOpened = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    Await.result(client.putTransaction(producerTxnOpened), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnOpened) shouldBe producerTxnOpened

    val updatedTTL1 = openedTTL
    val producerTxnUpdated1 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
    Await.result(client.putTransaction(producerTxnUpdated1), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnUpdated1) shouldBe producerTxnOpened

    val updatedTTL2 = openedTTL
    val producerTxnUpdated2 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL2)
    Await.result(client.putTransaction(producerTxnUpdated2), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnUpdated2) shouldBe producerTxnOpened

    val updatedTTL3 = openedTTL
    val producerTxnUpdated3 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    Await.result(client.putTransaction(producerTxnUpdated3), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnUpdated3) shouldBe producerTxnOpened

    val checkpointedTTL = 6
    val producerTxnCheckpointed = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    Await.result(client.putTransaction(producerTxnCheckpointed), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnCheckpointed) shouldBe producerTxnCheckpointed
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Updated->Updated->Checkpointed. Should return Invalid Transaction(due to expiration)" in {
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(client.putStream(stream), secondsWait.seconds)

    val openedTTL = 6
    val producerTxnOpened = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    Await.result(client.putTransaction(producerTxnOpened), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnOpened) shouldBe producerTxnOpened

    val updatedTTL1 = 4
    val producerTxnUpdated1 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
    Await.result(client.putTransaction(producerTxnUpdated1), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnUpdated1) shouldBe transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Opened, -1, updatedTTL1)


    val producerTxnInvalid = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Invalid, -1, 0)
    val updatedTTL2 = 2
    val producerTxnUpdated2 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL2)
    Await.result(client.putTransaction(producerTxnUpdated2), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnUpdated2) shouldBe producerTxnInvalid

    val updatedTTL3 = openedTTL
    val producerTxnUpdated3 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    Await.result(client.putTransaction(producerTxnUpdated3), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnUpdated3) shouldBe producerTxnInvalid

    val checkpointedTTL = 2
    val producerTxnCheckpointed = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    Await.result(client.putTransaction(producerTxnCheckpointed), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnCheckpointed) shouldBe producerTxnInvalid
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Cancel->Updated->Checkpointed. Should return Invalid Transaction(due to transaction with Cancel state)" in {
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(client.putStream(stream), secondsWait.seconds)

    val openedTTL = 6
    val producerTxnOpened = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    Await.result(client.putTransaction(producerTxnOpened), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnOpened) shouldBe producerTxnOpened

    val updatedTTL1 = 4
    val producerTxnUpdated1 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
    Await.result(client.putTransaction(producerTxnUpdated1), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnUpdated1) shouldBe transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Opened, -1, updatedTTL1)


    val producerTxnInvalid = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Invalid, -1, 0)
    val updatedTTL2 = 2
    val producerTxnCancel = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Cancel, -4, updatedTTL2)
    Await.result(client.putTransaction(producerTxnCancel), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnCancel) shouldBe producerTxnInvalid

    val updatedTTL3 = openedTTL
    val producerTxnUpdated3 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    Await.result(client.putTransaction(producerTxnUpdated3), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnUpdated3) shouldBe producerTxnInvalid

    val checkpointedTTL = 2
    val producerTxnCheckpointed = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    Await.result(client.putTransaction(producerTxnCheckpointed), secondsWait.seconds)

    Thread.sleep(5000)
    getProducerTransactionFromServer(producerTxnCheckpointed) shouldBe producerTxnInvalid
  }

}
