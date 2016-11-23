package zooKeeper

import com.twitter.finagle.Thrift
import com.twitter.util.Await
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import transactionService.server.TransactionServiceImpl

class ZKLeaderClient(address: String, sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends NodeCacheListener {
  @volatile var master: Option[String] = None
  val client = {
    val connection = CuratorFrameworkFactory.newClient(address, sessionTimeoutMillis, connectionTimeoutMillis, policy)
    connection.start()
    connection.blockUntilConnected()
    connection
  }

  scala.util.Try(client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(prefix,Array[Byte](0)))

  val nodeToWatch = new NodeCache(client, prefix, false)
  nodeToWatch.getListenable.addListener(this)

  def start = nodeToWatch.start()

  def close = {
    nodeToWatch.close()
    client.close()
  }

  override def nodeChanged(): Unit = {
    Option(nodeToWatch.getCurrentData) foreach (node =>
      master = Some(new String(nodeToWatch.getCurrentData.getData))
      )
  }
}


