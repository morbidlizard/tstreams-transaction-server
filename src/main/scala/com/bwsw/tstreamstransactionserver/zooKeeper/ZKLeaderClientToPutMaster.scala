package com.bwsw.tstreamstransactionserver.zooKeeper

import java.io.Closeable

import com.bwsw.tstreamstransactionserver.netty.server.Server
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.log4j.PropertyConfigurator
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

class ZKLeaderClientToPutMaster(endpoints: String, sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends Closeable {

  PropertyConfigurator.configure("src/main/resources/logServer.properties")
  private val logger = LoggerFactory.getLogger(classOf[Server])

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
