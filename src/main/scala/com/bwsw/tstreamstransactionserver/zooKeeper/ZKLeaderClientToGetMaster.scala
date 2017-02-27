package com.bwsw.tstreamstransactionserver.zooKeeper

import java.io.Closeable
import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.ZkNoConnectionException
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.curator.framework.CuratorFrameworkFactory
import org.slf4j.LoggerFactory


class ZKLeaderClientToGetMaster(endpoints: String, sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String, connectionStateListener: ConnectionStateListener)
  extends NodeCacheListener with Closeable {

  private val logger = LoggerFactory.getLogger(this.getClass)
  @volatile var master: Option[String] = None

  val client = {
    val connection = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(policy)
      .connectString(endpoints)
      .build()

    connection.getConnectionStateListenable.addListener(connectionStateListener)

    connection.start()
    val isConnected = connection.blockUntilConnected(connectionTimeoutMillis, TimeUnit.MILLISECONDS)
    if (isConnected) connection else throw new ZkNoConnectionException(endpoints)
  }

  val nodeToWatch = new NodeCache(client, prefix, false)
  nodeToWatch.getListenable.addListener(this)

  def start() = nodeToWatch.start()

  override def close(): Unit = {
    nodeToWatch.close()
    client.close()
  }

  override def nodeChanged(): Unit = {
    Option(nodeToWatch.getCurrentData) foreach {node =>
      val address = new String(node.getData)
      val isValidAddress = scala.util.Try(InetAddress.getByName(address.split(':').head))
      master = if (isValidAddress.isSuccess) Some(address) else {
        logger.info(s"$prefix data is corrupted!"); None
      }
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = close()
  })
}


