package zooKeeper

import java.io.Closeable
import java.net.InetAddress

import com.twitter.logging.{Level, Logger}
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}


//TODO think of implementation when there are many zkServers combined a Quorum
class ZKLeaderClient(endpoints: Seq[String], sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends NodeCacheListener with Closeable {
  private val logger = Logger.get(this.getClass)
  val client = {
    val connection = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(policy)
      .connectString(endpoints.head)
      .build()

    connection.start()
    connection
  }

  val nodeToWatch = new NodeCache(client, prefix, false)
  nodeToWatch.getListenable.addListener(this)

  def start() = nodeToWatch.start()

  override def close(): Unit = {
    nodeToWatch.close()
    client.close()
  }

  @volatile var master: Option[String] = None
  override def nodeChanged(): Unit = {
    Option(nodeToWatch.getCurrentData) foreach {node =>
      val address = new String(node.getData)
      val isValidAddress = scala.util.Try(InetAddress.getByName(address.split(':').head))
      master = if (isValidAddress.isSuccess) Some(address) else {
        logger.log(Level.INFO,s"$prefix data is corrupted!"); None
      }
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = close()
  })
}


