package it

import java.io.File
import java.util.concurrent.TimeUnit

import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.{HasTime, Server}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, CommonOptions, ServerOptions}
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class ManyClientsServerInterconnectionTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  val clientsNum = 2

  var zkTestServer: TestingServer = _
  var clients: Array[Client] = new Array[Client](clientsNum)
  var transactionServer: Server = _

  private val clientBuilder = new ClientBuilder()

  private object TestTimer extends HasTime {
    private val initialTime = System.currentTimeMillis()
    private var currentTime = initialTime

    override def getCurrentTime: Long = currentTime
    def resetTimer(): Unit = currentTime = initialTime
    def updateTime(newTime: Long) = currentTime = newTime
  }

  private val maxIdleTimeBeetwenRecords = 10
  private val commitLogToBerkeleyDBTaskDelay = 100

  private val serverAuthOptions = ServerOptions.AuthOptions()
  private val serverBootstrapOptions = ServerOptions.BootstrapOptions()
  private val serverReplicationOptions = ServerOptions.ServerReplicationOptions()
  private val serverStorageOptions = ServerOptions.StorageOptions()
  private val serverBerkeleyStorageOptions = ServerOptions.BerkeleyStorageOptions()
  private val serverRocksStorageOptions = ServerOptions.RocksStorageOptions()
  private val serverCommitLogOptions = ServerOptions.CommitLogOptions(maxIdleTimeBetweenRecords = maxIdleTimeBeetwenRecords, commitLogToBerkeleyDBTaskDelayMs = Int.MaxValue)
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
    (0 until clientsNum) foreach(index => clients(index) = clientBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build())
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

  private def chooseStreamRandomly(streams: IndexedSeq[com.bwsw.tstreamstransactionserver.rpc.Stream]) = streams(rand.nextInt(streams.length))

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

  "One client" should "put stream, then another client delete it, then then first client tries to put transactions but get throwable." in {
    val stream = getRandomStream

    val firstClient = clients(0)
    val secondClient = clients(1)

    Await.result(firstClient.putStream(stream), secondsWait.seconds)
    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened)
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val streamUpdated = stream.copy(description = Some("I overwrite previous one."))

    Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe false
    Await.result(secondClient.delStream(streamUpdated), secondsWait.seconds) shouldBe true

    //transactions are proccessed async
    Await.result(firstClient.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds) shouldBe true

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))
    Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    val fromID = producerTransactions.minBy(_.transactionID).transactionID
    val toID = producerTransactions.maxBy(_.transactionID).transactionID

    assertThrows[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist] {
      Await.result(firstClient.scanTransactions(stream.name, stream.partitions, fromID, toID), secondsWait.seconds).producerTransactions
    }
  }

  it should "put stream, then another client delete it, put with same name, then then first client tries to put transactions and it's okay." in {
    val stream = getRandomStream

    val firstClient = clients(0)
    val secondClient = clients(1)

    Await.result(firstClient.putStream(stream), secondsWait.seconds)
    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened)
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val streamUpdated = stream.copy(description = Some("I overwrite previous one."))

    Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe false
    Await.result(secondClient.delStream(streamUpdated), secondsWait.seconds) shouldBe true
    Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe true

    Await.result(firstClient.getStream(stream.name), secondsWait.seconds) should not be stream
    Await.result(firstClient.getStream(stream.name), secondsWait.seconds) shouldBe streamUpdated

    //transactions are proccessed async
    Await.result(firstClient.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds) shouldBe true

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))
    Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    val fromID = producerTransactions.minBy(_.transactionID).transactionID
    val toID = producerTransactions.maxBy(_.transactionID).transactionID

    Await.result(firstClient.scanTransactions(stream.name, stream.partitions, fromID, toID), secondsWait.seconds).producerTransactions should not be empty
  }


  it should "put stream, then another client put transactions, then first client tries to put transactions." in {
    val stream = getRandomStream

    val firstClient = clients(0)
    val secondClient = clients(1)

    Await.result(firstClient.putStream(stream), secondsWait.seconds)

    val streamUpdated = stream.copy(description = Some("I overwrite previous one."))
    Await.result(secondClient.putStream(streamUpdated), secondsWait.seconds) shouldBe false

    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(1))

    //transactions are proccessed async
    val producerTransaction1 = ProducerTransaction(stream.name, stream.partitions, TestTimer.getCurrentTime, TransactionStates.Opened, -1, Long.MaxValue)
    Await.result(secondClient.putProducerState(producerTransaction1), secondsWait.seconds) shouldBe true
    Await.result(secondClient.putProducerState(producerTransaction1.copy(state = TransactionStates.Checkpointed)), secondsWait.seconds) shouldBe true
    Await.result(firstClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe -1L
    Await.result(secondClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe -1L

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))
    Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogWriter writes the producer transactions to db
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
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))
    Await.result(firstClient.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    Await.result(firstClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe producerTransaction2.transactionID
    Await.result(secondClient.getLastCheckpointedTransaction(stream.name, stream.partitions), secondsWait.seconds) shouldBe producerTransaction2.transactionID
  }


}
