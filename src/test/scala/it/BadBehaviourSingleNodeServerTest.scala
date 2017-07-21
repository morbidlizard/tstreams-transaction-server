package it

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.{Message, SocketHostPortPair}
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandlerRouter
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKClient
import com.bwsw.tstreamstransactionserver.netty.server.ServerHandler
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServer
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{TransportOptions, _}
import io.netty.channel.ChannelHandlerContext
import org.apache.commons.io.FileUtils
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.slf4j.Logger
import util.Utils

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

class BadBehaviourSingleNodeServerTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterEach {

  var zkTestServer: TestingServer = _

  private val rand = scala.util.Random

  private def getRandomStream = new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
    override val name: String = rand.nextInt(10000).toString
    override val partitions: Int = rand.nextInt(10000)
    override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
    override val ttl: Long = Long.MaxValue
    override def zkPath: Option[String] = None
  }


  private val requestTimeoutMs = 500
  @volatile private var server: SingleNodeServer = _
  private val serverGotRequest = new AtomicInteger(0)

  private def serverHandler(requestHandlerChooser: RequestHandlerRouter,
                            executionContext: ServerExecutionContextGrids,
                            logger: Logger) =
    new ServerHandler(requestHandlerChooser, executionContext, logger)
    {
      override def invokeMethod(message: Message, ctx: ChannelHandlerContext): Unit = {
        serverGotRequest.getAndIncrement()
        Thread.sleep(requestTimeoutMs + 10)
        super.invokeMethod(message, ctx)
      }
    }


  private val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthenticationOptions()
  private val bootstrapOptions = BootstrapOptions()
  private val serverRoleOptions = ServerRoleOptions()
  private val serverReplicationOptions = ServerReplicationOptions()
  private val storageOptions = StorageOptions()
  private val rocksStorageOptions = RocksStorageOptions()
  private val packageTransmissionOptions = TransportOptions()
  private val commitLogOptions = CommitLogOptions()
  private val subscriberUpdateOptions = ServerOptions.SubscriberUpdateOptions()
  def startTransactionServer(): SingleNodeServer = {
    val address = zkTestServer.getConnectString
    val zookeeperOptions = ZookeeperOptions(endpoints = address)
    server = new SingleNodeServer(
      authOptions, zookeeperOptions,
      bootstrapOptions,
      serverRoleOptions,
      serverReplicationOptions,
      storageOptions,
      rocksStorageOptions,
      commitLogOptions,
      packageTransmissionOptions,
      subscriberUpdateOptions,
      serverHandler
    )
    val latch = new CountDownLatch(1)
    new Thread(() => {
      server.start(latch.countDown())
    }).start()

    latch.await()
    server
  }


  private val secondsWait = 5

  private val masterElectionPrefix = "/tts/master_election"


  override def beforeEach(): Unit = {
    zkTestServer = new TestingServer(true)
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
  }

  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
    serverGotRequest.set(0)
    zkTestServer.close()
  }

  "Client" should "send request with such ttl that it will never converge to a stable state due to the pipeline." in {
    startTransactionServer()

    val authOpts: AuthOptions =
      com.bwsw.tstreamstransactionserver.options.ClientOptions.AuthOptions()
    val address =
      zkTestServer.getConnectString
    val zookeeperOpts: ZookeeperOptions =
      com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = address)

    val retryDelayMsForThat = 100
    val retryCount = 10
    val connectionOpts: ConnectionOptions =
      com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(
      requestTimeoutMs = requestTimeoutMs,
      retryDelayMs = retryDelayMsForThat,
      connectionTimeoutMs = 1000,
      requestTimeoutRetryCount = retryCount
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

    val serverRequestCounter = serverGotRequest.get()
    val clientRequestCounter = clientTimeoutRequestCounter.get()

    server.shutdown()
    client.shutdown()

    //A Client hook works only on a request, so, if request fails - the hook would stop to work.
    //Taking in account all of the above, counter of serverRequestCounter may show that the client send one request less.
    (serverRequestCounter - clientRequestCounter) should be <= 1
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

    server.shutdown()
    client.shutdown()
  }


  it should "throw an user defined exception on overriding onServerConnectionLost method" in {
    val authOpts: AuthOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.AuthOptions()
    val zookeeperOpts: ZookeeperOptions = com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    val connectionOpts: ConnectionOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(connectionTimeoutMs = 100)

    val port = Utils.getRandomPort
    val socket = SocketHostPortPair
      .validateAndCreate("127.0.0.1", port)
      .get

    val zKLeaderClientToPutMaster = new ZKClient(
      endpoints = zkTestServer.getConnectString,
      zookeeperOpts.sessionTimeoutMs,
      zookeeperOpts.connectionTimeoutMs,
      new RetryForever(zookeeperOpts.retryDelayMs)
    )

    val masterElector =
      zKLeaderClientToPutMaster
        .masterElector(
          socket,
          zookeeperOpts.prefix,
          masterElectionPrefix
        )


    masterElector.start()

    val promise = Promise[Unit]()
    class MyThrowable extends Exception("My exception")
    val client = new Client(connectionOpts, authOpts, zookeeperOpts) {
      override def onServerConnectionLost(): Unit = {
        promise.tryFailure(new MyThrowable)
      }
    }

    assertThrows[MyThrowable] {
      Await.result(promise.future, 5.seconds)
    }

    client.shutdown()
    masterElector.stop()
    zKLeaderClientToPutMaster.close()
  }
}
