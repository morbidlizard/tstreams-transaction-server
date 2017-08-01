package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.io.Closeable

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter

class LeaderSelector(client: CuratorFramework,
                     electionPath: String)
  extends LeaderSelectorListenerAdapter
    with Closeable
    with Electable {

  private val leaderSelector = {
    val leader =
      new org.apache.curator.framework.recipes.leader.LeaderSelector(
        client,
        electionPath,
        this
      )
    leader.autoRequeue()
    leader.start()

    leader
  }

  override def hasLeadership: Boolean =
    leaderSelector.hasLeadership

  @throws[Exception]
  override def takeLeadership(client: CuratorFramework): Unit = {
    this.synchronized {
      try {
        while (true) this.wait()
      }
      catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
      }
    }
  }

  override def close(): Unit = stopParticipateInElection()

  override def stopParticipateInElection(): Unit =
    leaderSelector.close()
}
