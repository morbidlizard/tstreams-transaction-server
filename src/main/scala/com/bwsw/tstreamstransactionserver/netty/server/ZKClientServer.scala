package com.bwsw.tstreamstransactionserver.netty.server

import java.io.Closeable
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.exception.Throwable.{InvalidSocketAddress, ZkNoConnectionException}
import com.bwsw.tstreamstransactionserver.netty.InetSocketAddressClass
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.StreamDatabaseZK
import com.google.common.net.InetAddresses
import io.netty.resolver.dns.DnsNameResolver
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.curator.framework.recipes.leader.Participant
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.data.ACL
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.Try

class ZKClientServer(serverAddress: String,
                     serverPort: Int,
                     endpoints: String,
                     sessionTimeoutMillis: Int,
                     connectionTimeoutMillis: Int,
                     policy: RetryPolicy
                    ) extends Closeable
{

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val socketAddress: String =
    if (InetSocketAddressClass.isValidSocketAddress(serverAddress, serverPort)) s"$serverAddress:$serverPort"
    else throw new InvalidSocketAddress(s"Invalid socket address $serverAddress:$serverPort")

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

  final def fileIDGenerator(path: String) = new FileIDGenerator(path)
  final class FileIDGenerator(path: String) {
    private val distributedAtomicLong = new DistributedAtomicLong(client, path, policy)
    distributedAtomicLong.initialize(-1L)

    @tailrec
    def current: Long = {
      val operation = distributedAtomicLong.get()
      if (operation.succeeded()) {
        operation.postValue()
      } else {
        current
      }
    }

    def increment: Long = {
      val operation = distributedAtomicLong.increment()
      if (operation.succeeded()) {
        val newID = operation.postValue()
        newID
      }
      else throw new Exception(s"Can't increment counter by 1: previous was ${operation.preValue()} but now it's ${operation.postValue()} ")
    }
  }

  def streamDatabase(path: String) = new StreamDatabaseZK(client, path)


//  final def replicationGroup(path: String) = new ReplicationGroup(path)
//  final class ReplicationGroup(path: String) {
//    import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheListener}
//    import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
//    import scala.collection.JavaConverters._
//
//    final class Listener(listener: PathChildrenCacheListener) {
//      private val cache = new PathChildrenCache(client, path, true)
//      cache.getListenable.addListener(listener)
//      cache.start()
//
//      def close(): Unit = cache.close()
//    }
//
//    final class Election(listener: LeaderLatchListener) {
//      private val leaderLatch = new LeaderLatch(client, path, socketAddress, LeaderLatch.CloseMode.NOTIFY_LEADER)
//      leaderLatch.addListener(listener)
//
//      def participants: Iterable[Participant] = leaderLatch.getParticipants.asScala
//
//      def join(): Unit = leaderLatch.start()
//      def leave(): Unit = leaderLatch.close(LeaderLatch.CloseMode.NOTIFY_LEADER)
//    }
//
//    def election(listener: LeaderLatchListener) = new Election(listener)
//    def listener(listener: PathChildrenCacheListener) = new Listener(listener)
//  }


  final def putSocketAddress(path: String): Try[String] = {
    scala.util.Try(client.delete().forPath(path))
    scala.util.Try{
      val permissions = new util.ArrayList[ACL]()
      permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
      client.create().creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .withACL(permissions)
        .forPath(path, socketAddress.getBytes())
    }
  }

  override def close(): Unit = scala.util.Try(client.close())
}