package zooKeeper

import java.io.{Closeable, IOException}
import java.util.concurrent.TimeUnit

import com.twitter.logging.{Level, Logger, Logging}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter, Participant}
import org.apache.curator.retry.ExponentialBackoffRetry
import zooKeeper.LeaderSelectorByPriorityClient.Priority

import scala.annotation.tailrec
import scala.util.{Failure, Success}


// create a leader selector using the given path for management
// all participants in a given leader selection must use the same path
// ExampleClient here is also a LeaderSelectorListener but this isn't required
class LeaderSelectorByPriorityClient(addressZooKeeper: String,val path: String,val id: String, val priority: LeaderSelectorByPriorityClient.Priority.Value)
  extends LeaderSelectorListenerAdapter
  with Closeable
{
  private val client = {
    val clnt = CuratorFrameworkFactory.newClient(addressZooKeeper, new ExponentialBackoffRetry(100, 2))
    clnt.start()
    clnt.blockUntilConnected()
    clnt
  }
  private val leaderSelector: LeaderSelector =  new LeaderSelector(client, s"$path/$priority", this)
  private val resettableCountDownLatch = new ResettableCountDownLatch(1)
  private val logger = Logger.get(classOf[LeaderSelectorByPriorityClient])

  override def equals(other: Any): Boolean = other match {
    case that: LeaderSelectorByPriorityClient =>
      path == that.path &&
        id == that.id
    case _ => false
  }

  LeaderSelectorByPriorityClient.Priority.values foreach { value =>
    scala.util.Try(client.create().creatingParentsIfNeeded().forPath(s"$path/$value"))
  }

  leaderSelector.setId(id)
  leaderSelector.autoRequeue()

  @throws[IOException]
  def start():Unit = {
    // the selection for this instance doesn't start until the leader selector is started
    // leader selection is done in the background so this call to leaderSelector.start() returns immediately
    //if (!IS_STARTED) {
      leaderSelector.start()
    //  IS_STARTED = true
   // }
  }

  def hasLeadership: Boolean = getLeader == id
  def hasNotLeadership: Boolean = !hasLeadership

  final def getLeader: String = {
    import org.apache.zookeeper.KeeperException.NoNodeException
    val triesNumber = 20
    def timeToSleep = TimeUnit.MILLISECONDS.sleep(30L)
    @tailrec
    def helper(participant: LeaderSelector, times: Int): String = {
      timeToSleep
      if (participant.getParticipants.size() == 0) ""
      else scala.util.Try(participant.getLeader) match {
        case Success(leader) =>
          if (leader.getId == "") {
            helper(participant, times - 1)
          } else leader.getId
        case Failure(error) => error match {
          case noNode: NoNodeException =>
            helper(participant, times - 1)
          case _ => throw error
        }
      }
    }
    if (priority == LeaderSelectorByPriorityClient.Priority.Normal)
      helper(new LeaderSelector(client, s"$path/${LeaderSelectorByPriorityClient.Priority.Low}", this), triesNumber)
    else
      helper(new LeaderSelector(client, s"$path/${LeaderSelectorByPriorityClient.Priority.Normal}", this), triesNumber)
  }

  def release() = {
    //resettableCountDownLatch.countDown()
    leaderSelector.close()
    def timeToSleep = TimeUnit.MILLISECONDS.sleep(30L)
    //Thread sleep makes a differ ,otherwise it shouldn't work properly
   // resettableCountDownLatch.countDown()
    timeToSleep
   // resettableCountDownLatch.reset
  }

  @throws[IOException]
  def close() {
    scala.util.Try(leaderSelector.close()) match {
      case Success(_) => ()
      case Failure(error) => error match {
        case _: IllegalMonitorStateException => logger.log(Level.INFO,"Need to update zookeeper client version to 2.11.1")
        case _: java.lang.IllegalStateException => logger.log(Level.INFO,"Client connection is already closed")
      }
    }
  }

  @throws[Exception]
  def takeLeadership(client: CuratorFramework) = {
    try {
      resettableCountDownLatch.await()
    } catch {
      case _: java.lang.InterruptedException => close()
    }
  }
}

object LeaderSelectorByPriorityClient extends App {
  object Priority extends Enumeration {
    val Normal, Low = Value
  }

//  val connectionString = "172.17.0.2:2181"
//  val client1 = new LeaderSelectorByPriorityClient(
//    connectionString,"/stream","192.168.0.1", Priority.Low
//  )
//
//
//  val client2 = new LeaderSelectorByPriorityClient(
//    connectionString,"/stream","192.168.0.2", Priority.Normal
//  )
//
//  val client3 = new LeaderSelectorByPriorityClient(
//    connectionString,"/stream","192.168.0.3", Priority.Normal
//  )
//
// // client1.release()
//
//  client1.start()
//  client2.start()
//  client3.start()
//
//  client2.close()
//
//
//  println(client1.getLeader)
}