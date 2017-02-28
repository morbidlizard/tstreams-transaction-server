package it

import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.{Server, ServerHandler, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ClientBuilder
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.slf4j.Logger

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration._

class ServerTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  private val rand = scala.util.Random
  private def getRandomStream = new transactionService.rpc.Stream {
    override val name: String = rand.nextInt(10000).toString
    override val partitions: Int = rand.nextInt(10000)
    override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
    override val ttl: Long = Long.MaxValue
  }

  private val zkTestServer = new TestingServer(true)
  private val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
  private val zookeeperOptions = ZookeeperOptions(endpoints = zkTestServer.getConnectString)
  private val bootstrapOptions = BootstrapOptions()
  private val storageOptions   = StorageOptions()
  private val serverReplicationOptions = ServerReplicationOptions()
  private val rocksStorageOptions = RocksStorageOptions()

  private val requestTimeoutMs = 100
  private var server: Server = _
  private def serverHandler(server: TransactionServer, context: ExecutionContextExecutorService,logger: Logger) = new ServerHandler(server, context, logger){
    override def invokeMethod(message: Message, inetAddress: String)(implicit context: ExecutionContext): Future[Message] = {
      Thread.sleep(requestTimeoutMs)
      super.invokeMethod(message, inetAddress)
    }
  }
  def startTransactionServer() = new Thread(() => {
    server = new Server(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, serverHandler)
    server.start()
  }).start()


  private val secondsWait = 5

  it should "send request with such ttl that it will never converge to a stable state due to the pipeline." in {
    startTransactionServer()
    Thread.sleep(300)

    val client = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withConnectionOptions(ConnectionOptions(requestTimeoutMs = requestTimeoutMs))
      .build()

    val stream = getRandomStream

    assertThrows[java.util.concurrent.TimeoutException] {
      Await.result(client.putStream(stream), secondsWait.seconds)
    }

    server.shutdown()
    client.shutdown()
  }

  it should "throw an user defined exception on overriding onRequestTimeout method" in {
    startTransactionServer()
    Thread.sleep(300)

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
    startTransactionServer()
    Thread.sleep(300)

    val authOpts: AuthOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.AuthOptions()
    val zookeeperOpts: ZookeeperOptions = com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    val connectionOpts: ConnectionOptions = com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions(connectionTimeoutMs = 5)

    class MyThrowable extends Exception("My exception")

    val client = new Client(connectionOpts, authOpts, zookeeperOpts) {
      override def onServerConnectionLost(): Unit = throw new MyThrowable
    }

    val stream = getRandomStream

    assertThrows[MyThrowable] {
      Await.result(client.putStream(stream), secondsWait.seconds)
    }

    server.shutdown()
    client.shutdown()
  }

}
