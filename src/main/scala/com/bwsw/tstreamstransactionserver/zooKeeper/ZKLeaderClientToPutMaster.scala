package com.bwsw.tstreamstransactionserver.zooKeeper

import java.io.Closeable
import java.util
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.{InvalidSocketAddress, ZkNoConnectionException}
import com.google.common.net.InetAddresses
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.apache.zookeeper.data.ACL
import org.slf4j.LoggerFactory

class ZKLeaderClientToPutMaster(endpoints: String, sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends Closeable {

  private val logger = LoggerFactory.getLogger(this.getClass)

  val client = {
    val connection = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(policy)
      .connectString(endpoints)
      .build()

    connection.start()
    val isConnected = connection.blockUntilConnected(connectionTimeoutMillis, TimeUnit.MILLISECONDS)
    if (isConnected) connection else throw new ZkNoConnectionException(endpoints)
  }


  def putSocketAddress(inetAddress: String, port: Int) = {
    val socketAddress =
      if (inetAddress != null && InetAddresses.isInetAddress(inetAddress) && port.toInt > 0 && port.toInt < 65536)
        s"$inetAddress:$port"
      else
        throw new InvalidSocketAddress(s"Invalid socket address $inetAddress:$port")

    scala.util.Try(client.delete().deletingChildrenIfNeeded().forPath(prefix))
    scala.util.Try{
      val permissions = new util.ArrayList[ACL]()
      permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))

      client.create().creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .withACL(permissions)
        .forPath(prefix, socketAddress.getBytes())
    }
  }

  override def close(): Unit = client.close()
}
