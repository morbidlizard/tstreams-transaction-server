package zooKeeper

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.zookeeper.CreateMode

class ZKLeaderServer(address: Seq[String], sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String) {
  val client = {
    val connection = CuratorFrameworkFactory.newClient(address.head, sessionTimeoutMillis, connectionTimeoutMillis, policy)
    connection.start()
    connection.blockUntilConnected()
    connection
  }
  scala.util.Try(client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(prefix,Array[Byte](0)))

  def putData(data: Array[Byte]) = client.setData().forPath(prefix, data)

  def close = client.close()
}
