package it

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.{Server, ServerHandler, TransactionServer, ZKLeaderClientToPutMaster}
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{TransportOptions, _}
import org.apache.commons.io.FileUtils
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

class BadBehaviourServerTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val rand = scala.util.Random

  private def getRandomStream = new com.bwsw.tstreamstransactionserver.rpc.Stream {
    override val name: String = rand.nextInt(10000).toString
    override val partitions: Int = rand.nextInt(10000)
    override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
    override val ttl: Long = Long.MaxValue
  }

  private val zkTestServer = new TestingServer(false)
  private val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
  private val zookeeperOptions = ZookeeperOptions(endpoints = zkTestServer.getConnectString)
  private val bootstrapOptions = BootstrapOptions()
  private val serverReplicationOptions = ServerReplicationOptions()
  private val storageOptions = StorageOptions()
  private val berkeleyStorageOptions = BerkeleyStorageOptions()
  private val rocksStorageOptions = RocksStorageOptions()
  private val packageTransmissionOptions = TransportOptions()
  private val commitLogOptions = CommitLogOptions()

  private val requestTimeoutMs = 500
  @volatile private var server: Server = _
  private val serverGotRequest = new AtomicInteger(0)

  private def serverHandler(server: TransactionServer,
                            scheduledCommitLogImpl: ScheduledCommitLog,
                            packageTransmissionOptions: TransportOptions,
                            context: ExecutionContextExecutorService, logger: Logger) = new ServerHandler(server, scheduledCommitLogImpl, packageTransmissionOptions, context, logger) {
    override def invokeMethod(message: Message, inetAddress: String)(implicit context: ExecutionContext): Future[Message] = {
      serverGotRequest.getAndIncrement()
      Thread.sleep(requestTimeoutMs)
      super.invokeMethod(message, inetAddress)
    }
  }

  def startTransactionServer() = new Thread(() => {
    server = new Server(
      authOptions, zookeeperOptions,
      bootstrapOptions, serverReplicationOptions,
      storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions,
      packageTransmissionOptions,
      serverHandler
    )

    server.start()
  }).start()


  private val secondsWait = 5


  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
    val commitLogCatalogue = new CommitLogCatalogue(storageOptions.path)
    commitLogCatalogue.catalogues.foreach(catalogue => catalogue.deleteAllFiles())
    zkTestServer.start()
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
    val commitLogCatalogue = new CommitLogCatalogue(storageOptions.path)
    commitLogCatalogue.catalogues.foreach(catalogue => catalogue.deleteAllFiles())
    zkTestServer.close()
  }

  "Client" should "send request with such ttl that it will never converge to a stable state due to the pipeline." in {
    startTransactionServer()

    val retryDelayMsForThat = 100

    val authOpts: AuthOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.AuthOptions()
    val zookeeperOpts: ZookeeperOptions = com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    val connectionOpts: ConnectionOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(
      requestTimeoutMs = requestTimeoutMs,
      retryDelayMs = retryDelayMsForThat,
      connectionTimeoutMs = 5000
    )

    val clientTimeoutRequestCounter = new AtomicInteger(0)
    val client = new Client(connectionOpts, authOpts, zookeeperOpts) {
      // invoked on response
      override def onRequestTimeout(): Unit = {
        clientTimeoutRequestCounter.getAndIncrement()
      }
    }

    val stream = getRandomStream

    scala.util.Try(Await.ready(client.putStream(stream), secondsWait.seconds))

    server.shutdown()
    client.shutdown()


    val serverRequestCounter = serverGotRequest.get()
    val (trialsLeftBound, trialsRightBound) = {
      val trials = TimeUnit.SECONDS.toMillis(secondsWait).toInt / (requestTimeoutMs + retryDelayMsForThat)
      (trials - trials * 15 / 100, trials + trials * 15 / 100)
    }

    serverGotRequest.set(0)
    serverRequestCounter should be >= trialsLeftBound
    serverRequestCounter should be <= trialsRightBound

    //Client hook works only on a request, so, if request fails - the hook would stop to work.
    //Taking in account all of the above, counter of clientTimeoutRequestCounter may show that it send one request less.
    (serverRequestCounter - clientTimeoutRequestCounter.get()) should be <= 2 //authenticate gives one more request
  }

  it should "throw an user defined exception on overriding onRequestTimeout method" in {
    startTransactionServer()

    val authOpts: AuthOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.AuthOptions()
    val zookeeperOpts: ZookeeperOptions = com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    val connectionOpts: ConnectionOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(requestTimeoutMs = requestTimeoutMs)

    class MyThrowable extends Exception("My exception")

    val client = new Client(connectionOpts, authOpts, zookeeperOpts) {
      override def onRequestTimeout(): Unit = throw new MyThrowable
    }

    val stream = getRandomStream

    assertThrows[MyThrowable] {
      Await.result(client.putStream(stream), secondsWait.seconds)
    }

    serverGotRequest.set(0)
    server.shutdown()
    client.shutdown()
  }


  it should "throw an user defined exception on overriding onServerConnectionLost method" in {
    val authOpts: AuthOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.AuthOptions()
    val zookeeperOpts: ZookeeperOptions = com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    val connectionOpts: ConnectionOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(connectionTimeoutMs = 100)

    val zKLeaderClientToPutMaster = new ZKLeaderClientToPutMaster(
      endpoints = zkTestServer.getConnectString,
      zookeeperOpts.sessionTimeoutMs,
      zookeeperOpts.connectionTimeoutMs,
      new RetryForever(zookeeperOpts.retryDelayMs),
      zookeeperOpts.prefix
    )

    zKLeaderClientToPutMaster.putSocketAddress("127.0.0.1", 1000)

    class MyThrowable extends Exception("My exception")
    assertThrows[MyThrowable] {
      new Client(connectionOpts, authOpts, zookeeperOpts) {
        override def onServerConnectionLost(): Unit = throw new MyThrowable
      }
    }

    zKLeaderClientToPutMaster.close()
  }
}
