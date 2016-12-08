package zooKeeper

import java.io.Closeable
import java.util.concurrent.TimeUnit

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.zookeeper.CreateMode

class ZKLeaderServer(endpoints: String, sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends Closeable {
  val client = {
    val connection = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(policy)
      .connectString(endpoints)
      .build()

    connection.start()
    connection
  }
  scala.util.Try(client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(prefix, Array[Byte](0)))

  def putData(data: Array[Byte]) = client.setData().forPath(prefix, data)

  override def close(): Unit = client.close()

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = close()
  })
}
