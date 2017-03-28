package it

import java.io.File
import java.util.concurrent.TimeUnit

import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.{Server, Time}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, CommonOptions, ServerOptions}
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ManyClientsServerInterconnectionTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  val clientsNum = 2

  var zkTestServer: TestingServer = _
  var clients: Array[Client] = new Array[Client](clientsNum)
  var transactionServer: Server = _

  private val clientBuilder = new ClientBuilder()

  private object TestTimer extends Time {
    private val initialTime = System.currentTimeMillis()
    private var currentTime = initialTime

    override def getCurrentTime: Long = currentTime

    def resetTimer(): Unit = currentTime = initialTime

    def updateTime(newTime: Long) = currentTime = newTime
  }

  private val maxIdleTimeBetweenRecords = 10
  private val commitLogToBerkeleyDBTaskDelay = 100

  private val serverAuthOptions = ServerOptions.AuthOptions()
  private val serverBootstrapOptions = ServerOptions.BootstrapOptions()
  private val serverReplicationOptions = ServerOptions.ServerReplicationOptions()
  private val serverStorageOptions = ServerOptions.StorageOptions()
  private val serverBerkeleyStorageOptions = ServerOptions.BerkeleyStorageOptions()
  private val serverRocksStorageOptions = ServerOptions.RocksStorageOptions()
  private val serverCommitLogOptions = ServerOptions.CommitLogOptions(maxIdleTimeBetweenRecords = maxIdleTimeBetweenRecords, commitLogToBerkeleyDBTaskDelayMs = Int.MaxValue)
  private val serverPackageTransmissionOptions = ServerOptions.PackageTransmissionOptions()

  def startTransactionServer() = new Thread(() => {
    val serverZookeeperOptions = CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    transactionServer = new Server(
      authOpts = serverAuthOptions,
      zookeeperOpts = serverZookeeperOptions,
      serverOpts = serverBootstrapOptions,
      serverReplicationOpts = serverReplicationOptions,
      storageOpts = serverStorageOptions,
      berkeleyStorageOptions = serverBerkeleyStorageOptions,
      rocksStorageOpts = serverRocksStorageOptions,
      commitLogOptions = serverCommitLogOptions,
      packageTransmissionOpts = serverPackageTransmissionOptions,
      timer = TestTimer
    )
    transactionServer.start()
  }).start()


  override def beforeEach(): Unit = {
    TestTimer.resetTimer()
    zkTestServer = new TestingServer(true)
    startTransactionServer()
    (0 until clientsNum) foreach (index => clients(index) = clientBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build())
    val commitLogCatalogue = new CommitLogCatalogue(serverStorageOptions.path)
    commitLogCatalogue.catalogues.foreach(catalogue => FileUtils.deleteDirectory(catalogue.dataFolder))
  }

  override def afterEach() {
    TestTimer.resetTimer()
    clients.foreach(_.shutdown())
    transactionServer.shutdown()
    zkTestServer.close()
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + "/" + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + "/" + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + "/" + serverStorageOptions.metadataDirectory))
    val commitLogCatalogue = new CommitLogCatalogue(serverStorageOptions.path)
    commitLogCatalogue.catalogues.foreach(catalogue => FileUtils.deleteDirectory(catalogue.dataFolder))
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

  private def getRandomStream =
    new com.bwsw.tstreamstransactionserver.rpc.Stream {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
    }

  private def getRandomProducerTransaction(streamObj: com.bwsw.tstreamstransactionserver.rpc.Stream,
                                           transactionState: TransactionStates = TransactionStates(rand.nextInt(TransactionStates.list.length) + 1),
                                           id: Long = System.nanoTime()) =
    new ProducerTransaction {
      override val transactionID: Long = id
      override val state: TransactionStates = transactionState
      override val stream: String = streamObj.name
      override val ttl: Long = Long.MaxValue
      override val quantity: Int = -1
      override val partition: Int = streamObj.partitions
    }

  private def getRandomConsumerTransaction(streamObj: com.bwsw.tstreamstransactionserver.rpc.Stream) =
    new ConsumerTransaction {
      override val transactionID: Long = scala.util.Random.nextLong()
      override val name: String = rand.nextInt(10000).toString
      override val stream: String = streamObj.name
      override val partition: Int = streamObj.partitions
    }


  val secondsWait = 5

  "One client" should "put stream, then another client should delete it. After that the first client tries to put transactions and gets an exception." in {
    val stream = getRandomStream

    val firstClient = clients(0)
    val secondClient = clients(1)

    Await.result(firstClient.putStream(stream), secondsWait.seconds)
    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened)
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val streamUpdated = stream.copy(description = Some("I overwrite a previous one."))

    Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe false
    Await.result(secondClient.delStream(streamUpdated), secondsWait.seconds) shouldBe true

    //transactions are processed in the async mode
    Await.result(firstClient.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds) shouldBe true

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBetweenRecords))
    Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    val fromID = producerTransactions.minBy(_.transactionID).transactionID
    val toID = producerTransactions.maxBy(_.transactionID).transactionID

    assertThrows[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist] {
      Await.result(firstClient.scanTransactions(stream.name, stream.partitions, fromID, toID), secondsWait.seconds).producerTransactions
    }
  }

  "One client" should "put stream, then another client should delete it and put with the same name. " +
    "After that the first client should put transactions and get them back in the appropriate quantity." in {
    val stream = getRandomStream
    val txnNumber = 100
    val firstClient = clients(0)
    val secondClient = clients(1)

    Await.result(firstClient.putStream(stream), secondsWait.seconds)
    val producerTransactions = Array.fill(txnNumber)(getRandomProducerTransaction(stream, TransactionStates.Opened))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val streamUpdated = stream.copy(description = Some("I overwrite a previous one."))

    Await.result(secondClient.delStream(streamUpdated), secondsWait.seconds) shouldBe true
    Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe true

    val currentStream = Await.result(firstClient.getStream(stream.name), secondsWait.seconds)
    currentStream should not be stream
    currentStream shouldBe streamUpdated

    //transactions are processed in the async mode
    Await.result(firstClient.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds) shouldBe true

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBetweenRecords))
    Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    val fromID = producerTransactions.head.transactionID
    val toID = producerTransactions.last.transactionID

    Await.result(firstClient.scanTransactions(stream.name, stream.partitions, fromID, toID), secondsWait.seconds).producerTransactions should have size txnNumber
  }


  "One client" should "put stream, then another client should put transactions on a stream on a partition. " +
    "After that the first client tries to put transactions on the stream on the partition and clients should get the same last checkpointed transaction." in {
    val stream = getRandomStream

    val firstClient = clients(0)
    val secondClient = clients(1)

    Await.result(firstClient.putStream(stream), secondsWait.seconds)

    val streamUpdated = stream.copy(description = Some("I overwrite a previous one."))
    Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe false

    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(1))

    //transactions are processed in the async mode
    val producerTransaction1 = ProducerTransaction(stream.name, stream.partitions, TestTimer.getCurrentTime, TransactionStates.Opened, -1, Long.MaxValue)

    Await.result(secondClient.putProducerState(producerTransaction1), secondsWait.seconds) shouldBe true
    Await.result(secondClient.putProducerState(producerTransaction1.copy(state = TransactionStates.Checkpointed)), secondsWait.seconds) shouldBe true
    Await.result(firstClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe -1L
    Await.result(secondClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe -1L

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBetweenRecords))
    Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    Await.result(firstClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe producerTransaction1.transactionID
    Await.result(secondClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe producerTransaction1.transactionID

    val producerTransaction2 = ProducerTransaction(stream.name, stream.partitions, TestTimer.getCurrentTime, TransactionStates.Opened, -1, Long.MaxValue)
    Await.result(secondClient.putProducerState(producerTransaction2.copy()), secondsWait.seconds) shouldBe true
    Await.result(secondClient.putProducerState(producerTransaction2.copy(state = TransactionStates.Checkpointed)), secondsWait.seconds) shouldBe true
    Await.result(secondClient.putProducerState(producerTransaction2.copy(state = TransactionStates.Opened, transactionID = TestTimer.getCurrentTime + 1L)), secondsWait.seconds) shouldBe true

    Await.result(firstClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe producerTransaction1.transactionID
    Await.result(secondClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe producerTransaction1.transactionID


    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBetweenRecords))
    Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    Await.result(firstClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe producerTransaction2.transactionID
    Await.result(secondClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe producerTransaction2.transactionID
  }

  "One client" should "put transaction data, another one should put the data with intersecting keys, and, as consequence, overwrite values." in {
    val stream = getRandomStream

    val dataAmount = 10

    val firstClient = clients(0)
    val secondClient = clients(1)

    Await.result(firstClient.putStream(stream), secondsWait.seconds)
    val producerTransaction = getRandomProducerTransaction(stream)

    val data1 = Array.fill(dataAmount)(("a" + new String(rand.nextInt(100000).toString)).getBytes)
    Await.result(firstClient.putTransactionData(stream.name, stream.partitions, producerTransaction.transactionID, data1, 0), secondsWait.seconds) shouldBe true
    val data1Retrieved = Await.result(firstClient.getTransactionData(stream.name, stream.partitions, producerTransaction.transactionID, 0, 10), secondsWait.seconds)

    val data2 = Array.fill(dataAmount)(("b" + new String(rand.nextInt(100000).toString)).getBytes)
    Await.result(secondClient.putTransactionData(stream.name, stream.partitions, producerTransaction.transactionID, data2, 0), secondsWait.seconds) shouldBe true
    val data2Retrieved = Await.result(secondClient.getTransactionData(stream.name, stream.partitions, producerTransaction.transactionID, 0, 10), secondsWait.seconds)

    data1Retrieved should not contain theSameElementsInOrderAs(data2Retrieved)
  }
}
