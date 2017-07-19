package it

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.SingleNodeServer
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, CommonOptions, ServerOptions}
import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SingleNodeServerConsumerTransactionNotificationTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  var zkTestServer: TestingServer = _
  var client: Client = _
  var transactionServer: SingleNodeServer = _

  private val clientBuilder = new ClientBuilder()

  private val maxIdleTimeBetweenRecordsMs = 1000

  private val commitLogToBerkeleyDBTaskDelayMs = 100

  private val serverAuthOptions = ServerOptions.AuthenticationOptions()
  private val serverBootstrapOptions = ServerOptions.BootstrapOptions()
  private val serverRoleOptions = ServerOptions.ServerRoleOptions()
  private val serverReplicationOptions = ServerOptions.ServerReplicationOptions()
  private val serverStorageOptions = ServerOptions.StorageOptions()
  private val serverRocksStorageOptions = ServerOptions.RocksStorageOptions()
  private val serverCommitLogOptions = ServerOptions.CommitLogOptions(closeDelayMs = commitLogToBerkeleyDBTaskDelayMs)
  private val serverPackageTransmissionOptions = ServerOptions.TransportOptions()
  private val subscriberUpdateOptions = ServerOptions.SubscriberUpdateOptions()

  def startTransactionServer(): SingleNodeServer = {
    val serverZookeeperOptions = CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    transactionServer = new SingleNodeServer(
      authenticationOpts = serverAuthOptions,
      zookeeperOpts = serverZookeeperOptions,
      serverOpts = serverBootstrapOptions,
      serverRoleOptions = serverRoleOptions,
      serverReplicationOpts = serverReplicationOptions,
      storageOpts = serverStorageOptions,
      rocksStorageOpts = serverRocksStorageOptions,
      commitLogOptions = serverCommitLogOptions,
      packageTransmissionOpts = serverPackageTransmissionOptions,
      subscriberUpdateOptions
    )

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    latch.await()
    transactionServer
  }

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRawDirectory))

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
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRawDirectory))
  }

  private val rand = scala.util.Random
  private def getRandomStream =
    new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
      override val zkPath: Option[String] = None
    }


  val secondsWait = 5

  "Client" should "put consumerCheckpoint and get a transaction id back." in {
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
