package it

import java.io.File
import java.util.concurrent.TimeUnit

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

class CommitLogOffsetTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  var zkTestServer: TestingServer = _
  var client: Client = _
  var transactionServer: Server = _

  val clientsNum = 2
  private val clientBuilder = new ClientBuilder()

  private object TestTimer extends Time {
    private val initialTime = System.currentTimeMillis()
    private var currentTime = initialTime

    override def getCurrentTime: Long = currentTime
    def resetTimer(): Unit = currentTime = initialTime
    def updateTime(newTime: Long) = currentTime = newTime
  }

  private val maxIdleTimeBetweenRecordsMs = 1000

  private val serverAuthOptions = ServerOptions.AuthOptions()
  private val serverBootstrapOptions = ServerOptions.BootstrapOptions()
  private val serverReplicationOptions = ServerOptions.ServerReplicationOptions()
  private val serverStorageOptions = ServerOptions.StorageOptions()
  private val serverBerkeleyStorageOptions = ServerOptions.BerkeleyStorageOptions()
  private val serverRocksStorageOptions = ServerOptions.RocksStorageOptions()
  private val serverCommitLogOptions = ServerOptions.CommitLogOptions(commitLogCloseDelayMs = Int.MaxValue)
  private val serverPackageTransmissionOptions = ServerOptions.TransportOptions()
  private val serverZookeeperSpecificOptions = ServerOptions.ZooKeeperOptions()

  def startTransactionServer(): Unit = new Thread(() => {
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
      zookeeperSpecificOpts = serverZookeeperSpecificOptions,
      timer = TestTimer
    )
    transactionServer.start()
  }).start()


  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))

    TestTimer.resetTimer()
    zkTestServer = new TestingServer(true)
    startTransactionServer()
    TimeUnit.MILLISECONDS.sleep(100L)
    client = clientBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()
  }

  override def afterEach() {
    TestTimer.resetTimer()
    client.shutdown()
    transactionServer.shutdown()
    zkTestServer.close()

    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))
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

  "getCommitLogOffsets" should "return -1 for currentProcessedCommitLog and 0 for currentConstructedCommitLog as there is created commit log file at initialization of server and it's not processed." in {
    val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
    result.currentProcessedCommitLog   shouldBe -1L
    result.currentConstructedCommitLog shouldBe 0L
  }

  it should "return 0 for currentProcessedCommitLog and 1 for currentConstructedCommitLog as there is created commit log file at initialization of server, it's closed and server creates a new commit log file and server processes the first one." in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactions = Array.fill(10)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened) :+
      getRandomProducerTransaction(stream).copy(state = TransactionStates.Opened)

    Await.result(client.putTransactions(producerTransactions, Seq()), secondsWait.seconds)

    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    transactionServer.scheduledCommitLogImpl.run()
    transactionServer.berkeleyWriter.run()

    val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
    result.currentProcessedCommitLog   shouldBe 0L
    result.currentConstructedCommitLog shouldBe 1L
  }

  it should "return -1 for currentProcessedCommitLog(as writer thread doesn't run on data) and 0 for currentConstructedCommitLog(as it's initialized, got some data, but not closed)" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactions = Array.fill(10)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened) :+
      getRandomProducerTransaction(stream).copy(state = TransactionStates.Opened)

    Await.result(client.putTransactions(producerTransactions, Seq()), secondsWait.seconds)

    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)

    val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
    result.currentProcessedCommitLog   shouldBe -1L
    result.currentConstructedCommitLog shouldBe 0L
  }

  it should "return -1 for currentProcessedCommitLog(as writer thread doesn't run on data) and 1 for currentConstructedCommitLog(as the first one is closed, and a next one is created)" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactions = Array.fill(10)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened) :+
      getRandomProducerTransaction(stream).copy(state = TransactionStates.Opened)

    Await.result(client.putTransactions(producerTransactions, Seq()), secondsWait.seconds)

    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    transactionServer.scheduledCommitLogImpl.run()

    val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
    result.currentProcessedCommitLog   shouldBe -1L
    result.currentConstructedCommitLog shouldBe 1L
  }

  it should "return -1 for currentProcessedCommitLog(as writer thread doesn't run on data) and 3 for currentConstructedCommitLog(as 3 commit log files are closed)" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    transactionServer.scheduledCommitLogImpl.run()
    transactionServer.scheduledCommitLogImpl.run()
    transactionServer.scheduledCommitLogImpl.run()

    val result = Await.result(client.getCommitLogOffsets(), secondsWait.seconds)
    result.currentProcessedCommitLog   shouldBe -1L
    result.currentConstructedCommitLog shouldBe 3L
  }
}
