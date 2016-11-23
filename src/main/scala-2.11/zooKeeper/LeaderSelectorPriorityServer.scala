package zooKeeper

import java.io.{Closeable, IOException}
import java.util.concurrent.TimeUnit

import com.twitter.logging.{Level, Logger}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.annotation.tailrec
import scala.util.{Failure, Success}




sealed abstract class LeaderSelectorPriority(addressZooKeeper: String, val priority: LeaderSelectorPriority.Priority.Value, val path: String)
  extends LeaderSelectorListenerAdapter with Closeable
{
  protected final val client = {
    val clnt = CuratorFrameworkFactory.newClient(addressZooKeeper, new ExponentialBackoffRetry(100, 2))
    clnt.start()
    clnt.blockUntilConnected()
    clnt
  }
  protected val leaderSelector: LeaderSelector =  new LeaderSelector(client, s"$path/$priority", this)
  protected val logger = Logger.get(this.getClass)

  LeaderSelectorPriority.Priority.values foreach { value =>
    scala.util.Try(client.create().creatingParentsIfNeeded().forPath(s"$path/$value"))
  }

  @throws[IOException]
  def close(): Unit

  @throws[Exception]
  protected def takeLeadership(client: CuratorFramework) = ()
}


class LeaderSelectorPriorityServer(addressZooKeeper: String, priority: LeaderSelectorPriority.Priority.Value, path: String, val id: String)
  extends LeaderSelectorPriority(addressZooKeeper, priority, path)
{
  leaderSelector.setId(id)
  leaderSelector.autoRequeue()

  private val resettableCountDownLatch = new ResettableCountDownLatch(1)

  override def equals(other: Any): Boolean = other match {
    case that: LeaderSelectorPriorityServer =>
      path == that.path &&
        id == that.id
    case _ => false
  }

  def isLeader = leaderSelector.hasLeadership

  def release() = {
    def timeToSleep = TimeUnit.MILLISECONDS.sleep(30L)
    resettableCountDownLatch.countDown()
    leaderSelector.close()
    //Thread sleep makes a differ ,otherwise it shouldn't work properly
    timeToSleep
    resettableCountDownLatch.reset
  }

  @throws[Exception]
  override def takeLeadership(client: CuratorFramework) = {
    try {
      resettableCountDownLatch.await()
    } catch {
      case _: java.lang.InterruptedException => close()
    }
  }

  @throws[IOException]
  def start():Unit = leaderSelector.start()

  @throws[IOException]
  def close() {
    scala.util.Try{
      leaderSelector.close()
      client.close()
    } match {
      case Success(_) => ()
      case Failure(error) => error match {
        case _: IllegalMonitorStateException => logger.log(Level.INFO,"Need to update zookeeper client version to 2.11.1")
        case _: java.lang.IllegalStateException => logger.log(Level.INFO,"Client connection is already closed")
        case error => logger.log(Level.ERROR, error, "Some error")
      }
    }
  }
}

class LeaderSelectorPriorityClient(addressZooKeeper: String, priority: LeaderSelectorPriority.Priority.Value, path: String)
  extends LeaderSelectorPriority(addressZooKeeper, priority, path) {
  leaderSelector.setId("")

  final def getLeader: Option[(String, LeaderSelectorPriority.Priority.Value)] = {
    def timeToSleep() = TimeUnit.MILLISECONDS.sleep(30L)
    import org.apache.zookeeper.KeeperException.NoNodeException
    val triesNumber = 20

    @tailrec
    def findLeader(participant: LeaderSelector, times: Int): String = {
      timeToSleep()
      if (participant.getParticipants.size() == 0) ""
      else scala.util.Try(participant.getLeader) match {
        case Success(leader) =>
          if (leader.getId == "") {
            findLeader(participant, times - 1)
          } else leader.getId
        case Failure(error) => error match {
          case noNode: NoNodeException =>
            findLeader(participant, times - 1)
          case _ => throw error
        }
      }
    }

    @tailrec
    def findLeaderByPriority(priorities: LeaderSelectorPriority.Priority.ValueSet, leader: Option[(String, LeaderSelectorPriority.Priority.Value)]): Option[(String, LeaderSelectorPriority.Priority.Value)] =
      if (priorities.isEmpty) leader
      else {
        val priority = priorities.head
        val (leaderId, leaderPriority) = (findLeader(new LeaderSelector(client, s"$path/$priority", this), triesNumber), priority)
        if (leaderId == "") findLeaderByPriority(priorities.tail, None)
        else Some((leaderId, leaderPriority))
      }
    findLeaderByPriority(LeaderSelectorPriority.Priority.values, None)
  }

  @throws[IOException] def close(): Unit = client.close()
}



object LeaderSelectorPriority {
  object Priority extends Enumeration {
    val Normal,Low = Value
  }
}
