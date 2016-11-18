package zooKeeper

import java.io.{Closeable, IOException}
import java.util.concurrent.TimeUnit

import com.twitter.logging.{Level, Logger, Logging}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter, Participant}

import scala.annotation.tailrec
import scala.util.{Failure, Success}


// create a leader selector using the given path for management
// all participants in a given leader selection must use the same path
// ExampleClient here is also a LeaderSelectorListener but this isn't required
class MyLeaderSelectorClient(client: CuratorFramework,val path: String,val id: String) extends LeaderSelectorListenerAdapter
  with Closeable
{
  private val leaderSelector: LeaderSelector =  new LeaderSelector(client, path, this)
  private val resettableCountDownLatch = new ResettableCountDownLatch(1)
  private val logger = Logger.get(classOf[MyLeaderSelectorClient])
  @volatile private var IS_STARTED = false

  override def equals(other: Any): Boolean = other match {
    case that: MyLeaderSelectorClient =>
      path == that.path &&
        id == that.id
    case _ => false
  }

  leaderSelector.setId(id)

  @throws[IOException]
  def start() = {
    // the selection for this instance doesn't start until the leader selector is started
    // leader selection is done in the background so this call to leaderSelector.start() returns immediately
    if (IS_STARTED) leaderSelector.requeue() else {leaderSelector.start(); IS_STARTED = true}
  }

  def requeue() = start()

  def isStarted = IS_STARTED

  def hasLeadership: Boolean = getLeader.getId == id
  def hasNotLeadership: Boolean = !hasLeadership

  final def getLeader: Participant = {
    import org.apache.zookeeper.KeeperException.NoNodeException
    def timeToSleep = TimeUnit.MILLISECONDS.sleep(30L)
    @tailrec
    def helper(times: Int): Participant = {
      scala.util.Try(leaderSelector.getLeader) match {
        case Success(leader) => if (leader.getId == "") {
          timeToSleep
          helper(times - 1)
        } else leader
        case Failure(error) => error match {
          case noNode: NoNodeException =>
            timeToSleep
            helper(times - 1)
          case _ => throw error
        }
      }
    }
    helper(20)
  }

  def release() = {
    def timeToSleep = TimeUnit.MILLISECONDS.sleep(30L)
    //Thread sleep makes a differ ,otherwise it shouldn't work properly
    resettableCountDownLatch.countDown()
    timeToSleep
    resettableCountDownLatch.reset
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