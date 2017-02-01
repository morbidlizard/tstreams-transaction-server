package com.bwsw.tstreamstransactionserver.zooKeeper

import java.io.Closeable
import java.net.InetAddress

import com.bwsw.tstreamstransactionserver.netty.client.Client
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory


class ZKLeaderClientToGetMaster(endpoints: String, sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends NodeCacheListener with Closeable {

  PropertyConfigurator.configure("src/main/resources/logClient.properties")
  private val logger = LoggerFactory.getLogger(classOf[Client])
  @volatile var master: Option[String] = None

  val client = {
    val connection = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(policy)
      .connectString(endpoints)
      .build()

    connection.start()
   // connection.blockUntilConnected()
    connection.getConnectionStateListenable.addListener(new ConnectionStateListener {
      override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
        newState match {
          case ConnectionState.LOST => master = None
          case _ => ()
        }
      }
    })
    connection
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


