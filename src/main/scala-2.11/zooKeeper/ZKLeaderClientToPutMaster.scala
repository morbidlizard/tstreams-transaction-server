package zooKeeper

import java.io.Closeable

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.zookeeper.CreateMode

class ZKLeaderClientToPutMaster(endpoints: String, sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends Closeable {
  val client = {
    val connection = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(policy)
      .connectString(endpoints)
      .build()

    connection.start()
    connection.blockUntilConnected()
    connection
  }

  def putData(data: Array[Byte]) = {
    scala.util.Try(client.delete().deletingChildrenIfNeeded().forPath(prefix))
    scala.util.Try(client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(prefix, data))
  }

  override def close(): Unit = client.close()

//  Runtime.getRuntime.addShutdownHook(new Thread {
//    override def run() = close()
//  })
}
