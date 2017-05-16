package it

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, CommonOptions, ServerOptions}
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class ServerConsumerTransactionNotificationTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  var zkTestServer: TestingServer = _
  var client: Client = _
  var transactionServer: Server = _

  private val clientBuilder = new ClientBuilder()

  private val maxIdleTimeBetweenRecordsMs = 1000

  private val commitLogToBerkeleyDBTaskDelayMs = 100

  private val serverAuthOptions = ServerOptions.AuthOptions()
  private val serverBootstrapOptions = ServerOptions.BootstrapOptions()
  private val serverReplicationOptions = ServerOptions.ServerReplicationOptions()
  private val serverStorageOptions = ServerOptions.StorageOptions()
  private val serverRocksStorageOptions = ServerOptions.RocksStorageOptions()
  private val serverCommitLogOptions = ServerOptions.CommitLogOptions(commitLogCloseDelayMs = commitLogToBerkeleyDBTaskDelayMs)
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
      rocksStorageOpts = serverRocksStorageOptions,
      commitLogOptions = serverCommitLogOptions,
      zookeeperSpecificOpts = serverZookeeperSpecificOptions,
      packageTransmissionOpts = serverPackageTransmissionOptions
    )
    transactionServer.start()
  }).start()


  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))

    zkTestServer = new TestingServer(true)
    startTransactionServer()
    client = clientBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()
  }

  override def afterEach() {
    client.shutdown()
    transactionServer.shutdown()
    zkTestServer.close()

    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))
  }

  private val rand = scala.util.Random
  private def getRandomStream =
    new com.bwsw.tstreamstransactionserver.rpc.Stream {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
    }


  val secondsWait = 5

  "Client" should "put consumerCheckpoint and get a transaction id back" in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

    val transactionId = 10L
    val checkpointName = "test-name"


    val latch = new CountDownLatch(1)
    transactionServer.notifyConsumerTransactionCompleted(consumerTransaction =>
      consumerTransaction.transactionID == transactionId && consumerTransaction.name == checkpointName, latch.countDown()
    )

    val consumerTransactionOuter = ConsumerTransaction(streamID, 1, transactionId, checkpointName)
    client.putConsumerCheckpoint(consumerTransactionOuter)

    latch.await(1, TimeUnit.SECONDS) shouldBe true
  }

  it should "shouldn't get notification." in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val transactionId = 10L
    val checkpointName = "test-name"

    val latch = new CountDownLatch(1)
    val id = transactionServer.notifyConsumerTransactionCompleted(consumerTransaction =>
      consumerTransaction.transactionID == transactionId && consumerTransaction.name == checkpointName, latch.countDown()
    )

    latch.await(1, TimeUnit.SECONDS) shouldBe false
    transactionServer.removeConsumerNotification(id) shouldBe true
  }

  it should "get notification about consumer checkpoint after using putTransactions method." in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

    val transactionId = 10L
    val checkpointName = "test-name"


    val latch = new CountDownLatch(1)
    transactionServer.notifyConsumerTransactionCompleted(consumerTransaction =>
      consumerTransaction.transactionID == transactionId, latch.countDown()
    )

    val consumerTransactionOuter = ConsumerTransaction(streamID, 1, transactionId, checkpointName)
    client.putTransactions(Seq(), Seq(consumerTransactionOuter))

    latch.await(1, TimeUnit.SECONDS) shouldBe true
  }

}
