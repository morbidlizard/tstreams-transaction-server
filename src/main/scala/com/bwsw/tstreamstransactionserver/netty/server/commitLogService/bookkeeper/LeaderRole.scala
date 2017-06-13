package com.bwsw.tstreamstransactionserver.netty.server.commitLogService.bookkeeper

import java.io.Closeable

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}

class LeaderRole(client: CuratorFramework,
                 electionPath: String
                )
  extends LeaderSelectorListenerAdapter
    with Closeable
    with ServerRole
{



  private val leaderSelector = {
    val leader = new LeaderSelector(client, electionPath, this)
    leader.autoRequeue()
    leader.start()

    leader
  }

  override def hasLeadership: Boolean =
    leaderSelector.hasLeadership

  @throws[Exception]
  override def takeLeadership(client: CuratorFramework): Unit = {
    this.synchronized {
      println("Becoming leader")
      try {
        while(true) this.wait()
      }
      catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
      }
    }
  }

  override def close(): Unit =
    leaderSelector.close()
}
