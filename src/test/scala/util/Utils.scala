package util

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.options.{CommonOptions, ServerBuilder}
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


  def startTransactionServer(builder: ServerBuilder): ZkSeverAndTransactionServer = {
    val zkTestServer = new TestingServer(true)

    val transactionServer = builder.withZookeeperOptions(
      CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    ).build()

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    latch.await()
    ZkSeverAndTransactionServer(zkTestServer, transactionServer)
  }
}
