package com.bwsw.tstreamstransactionserver.netty.server.zk

import java.util

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.data.ACL

import scala.util.Try

final class ZKMasterElector(curatorClient: CuratorFramework,
                            socket: SocketHostPortPair,
                            masterPrefix: String,
                            masterElectionPrefix: String)
  extends LeaderLatchListener {

  private val leaderLatch =
    new LeaderLatch(
      curatorClient,
      masterElectionPrefix,
      socket.toString
    )
  leaderLatch.addListener(this)

  private def putSocketAddress(): Try[String] = {
    scala.util.Try(curatorClient.delete().forPath(masterPrefix))
    scala.util.Try {
      val permissions = new util.ArrayList[ACL]()
      permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
      curatorClient.create().creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .withACL(permissions)
        .forPath(masterPrefix, socket.toString.getBytes())
    }
  }

  def leaderID: String =
    leaderLatch.getLeader.getId

  def start(): Unit =
    leaderLatch.start()

  def stop(): Unit =
    leaderLatch.close(
      LeaderLatch.CloseMode.NOTIFY_LEADER
    )

  override def isLeader(): Unit = {
    putSocketAddress()
  }

  override def notLeader(): Unit = {

  }
}
