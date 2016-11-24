package zooKeeper

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}


//TODO think of implementation when there are many zkServers combined a Quorum
class ZKLeaderClient(endpoints: Seq[String], sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends NodeCacheListener {
  @volatile var master: Option[String] = None
  val client = {
    val connection = CuratorFrameworkFactory.newClient(endpoints.head, sessionTimeoutMillis, connectionTimeoutMillis, policy)
    connection.start()
    connection.blockUntilConnected()
    connection
  }

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

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = close
  })
}


