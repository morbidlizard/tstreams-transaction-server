package util

import java.net.ServerSocket
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, SingleNodeServerBuilder}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.test.TestingServer

object Utils {
  private val sessionTimeoutMillis = 1000
  private val connectionTimeoutMillis = 1000

  def startZkServerAndGetIt: (TestingServer, CuratorFramework) = {
    val zkServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(new RetryNTimes(3, 100))
      .connectString(zkServer.getConnectString)
      .build()

    zkClient.start()
    zkClient.blockUntilConnected(100, TimeUnit.MILLISECONDS)

    (zkServer, zkClient)
  }

  private val rand = scala.util.Random

  def getRandomStream =
    new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
      override val zkPath: Option[String] = None
    }

  def getRandomPort: Int = {
    scala.util.Try {
      new ServerSocket(0)
    }.map { server =>
      val port = server.getLocalPort
      server.close()
      port
    }.get
  }

  def startTransactionServer(builder: SingleNodeServerBuilder): ZkSeverAndTransactionServer = {
    val zkTestServer = new TestingServer(true)
    val transactionServer = builder
      .withZookeeperOptions(
        builder.getZookeeperOptions.copy(endpoints = zkTestServer.getConnectString)
      )
      .withBootstrapOptions(
        builder.getBootstrapOptions.copy(bindPort = getRandomPort)
      )
      .build()

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    latch.await(3000, TimeUnit.SECONDS)
    ZkSeverAndTransactionServer(zkTestServer, transactionServer)
  }

  def startTransactionServerAndClient(serverBuilder: SingleNodeServerBuilder,
                                      clientBuilder: ClientBuilder): ZkSeverTxnServerTxnClient = {
    val zkTestServer = new TestingServer(true)
    val transactionServer = serverBuilder
      .withZookeeperOptions(
        serverBuilder.getZookeeperOptions.copy(endpoints = zkTestServer.getConnectString)
      )
//      .withServerStorageOptions(
//        serverBuilder.getStorageOptions.copy(Files.createTempDirectory("/tmp").toFile.getPath)
//      )
      .withBootstrapOptions(
        serverBuilder.getBootstrapOptions.copy(bindPort = getRandomPort)
      )
      .build()

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()
    latch.await(3000, TimeUnit.SECONDS)

    val client = new ClientBuilder()
      .withZookeeperOptions(
        ZookeeperOptions(endpoints = zkTestServer.getConnectString)
      )
      .build()

    ZkSeverTxnServerTxnClient(zkTestServer, transactionServer, client)
  }
}
