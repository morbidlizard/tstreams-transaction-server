package com.bwsw.tstreamstransactionserver.netty.server

import java.io.Closeable
import java.util
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.{InvalidSocketAddress, ZkNoConnectionException}
import com.google.common.net.InetAddresses
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.data.ACL
import org.slf4j.LoggerFactory

class ZKLeaderClientToPutMaster(endpoints: String, sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends Closeable {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val client = {
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

  final def fileIDGenerator(path: String, initValue: Long) = new FileIDGenerator(path, initValue)
  final class FileIDGenerator(path: String, initValue: Long) {
    private val distributedAtomicLong = new DistributedAtomicLong(client, path, policy)
    distributedAtomicLong.forceSet(initValue)
   // if (!distributedAtomicLong.initialize(initValue)) throw new Exception(s"Can't initialize counter by value $initValue.")

    def increment: Long = {
      val operation = distributedAtomicLong.increment()
      if (operation.succeeded()) operation.postValue()
      else throw new Exception(s"Can't increment counter by 1: previous was ${operation.preValue()} but now it's ${operation.postValue()} ")
    }
  }


  def putSocketAddress(inetAddress: String, port: Int) = {
    val socketAddress =
      if (ZKLeaderClientToPutMaster.isValidSocketAddress(inetAddress, port)) s"$inetAddress:$port"
      else throw new InvalidSocketAddress(s"Invalid socket address $inetAddress:$port")

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

  override def close(): Unit = scala.util.Try(client.close())
}

object ZKLeaderClientToPutMaster {
  def isValidSocketAddress(inetAddress: String, port: Int) = {
    if (inetAddress != null && InetAddresses.isInetAddress(inetAddress) && port.toInt > 0 && port.toInt < 65536) true
    else false
  }
}
