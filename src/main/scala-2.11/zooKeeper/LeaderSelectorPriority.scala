package zooKeeper

import java.io.Closeable

import LeaderSelectorPriority.Priority
import org.apache.curator.framework.CuratorFramework

import scala.collection.mutable.ArrayBuffer


class LeaderSelectorPriority(val path: String) extends Closeable {
  private val leaderSelectorsWithPriorities: ArrayBuffer[(MyLeaderSelectorClient, Priority.Value)] = new ArrayBuffer()

  final def getLeaderId = leaderSelectorsWithPriorities.headOption match {
    case Some(leader) => Some(leader._1.getLeader.getId)
    case None => None
  }

  def isAllLeaderSelectorsHaveTheSameLeader: Boolean = leaderSelectorsWithPriorities.map(_._1.getLeader).distinct.length == 1

  def close(): Unit = leaderSelectorsWithPriorities foreach (_._1.close())

  private def chooseLeader(currentLeader: MyLeaderSelectorClient, newLeader: MyLeaderSelectorClient) = {
    currentLeader.release()
    newLeader.requeue()
  }

  private def votersAndLeader: (
    ArrayBuffer[(MyLeaderSelectorClient, Priority.Value)],
      (MyLeaderSelectorClient, Priority.Value)
    ) = {
    val currentLeaderIndex = leaderSelectorsWithPriorities.indexWhere(_._1.hasLeadership)
    val currentLeader = leaderSelectorsWithPriorities(currentLeaderIndex)
    val votersWithoutLeader = leaderSelectorsWithPriorities - currentLeader
    (votersWithoutLeader, currentLeader)
  }

  final def addId(id: String, priority: LeaderSelectorPriority.Priority.Value, client: CuratorFramework): Unit = this.synchronized {
    val leader = new MyLeaderSelectorClient(client,path,id)

      if (!leaderSelectorsWithPriorities.exists(_._1.id == leader.id))
        leaderSelectorsWithPriorities += ((leader, priority))

    if (leaderSelectorsWithPriorities.size == 1) {
      leader.start()
    } else {

      val (_, currentLeader) = votersAndLeader
      processCreate(currentLeader,(leader,priority))
    }
  }

  final def removeId(id:String):Unit = this.synchronized {
    leaderSelectorsWithPriorities.find(_._1.id == id) match {
      case Some(leaderSelector) => {
        val (votersWithoutLeader, currentLeader) = votersAndLeader
        if (currentLeader == leaderSelector)
          processDelete(votersWithoutLeader, currentLeader, LeaderSelectorPriority.getRandomLeader)
        leaderSelector._1.close()
        leaderSelectorsWithPriorities -= leaderSelector
      }
      case None => None
    }
  }

  protected def processCreate(currentLeader: (MyLeaderSelectorClient, Priority.Value),
                              newLeader: (MyLeaderSelectorClient, Priority.Value)
                             ): Unit = {

    if (newLeader._2 == Priority.Normal && currentLeader._2 == Priority.Low)
      chooseLeader(currentLeader._1, newLeader._1)
  }


  protected def processDelete(voters: ArrayBuffer[(MyLeaderSelectorClient, Priority.Value)],
                              currentLeader: (MyLeaderSelectorClient, Priority.Value),
                              randomFunction: ArrayBuffer[MyLeaderSelectorClient] => MyLeaderSelectorClient
                             ): Unit = {
    if (voters.isEmpty) currentLeader._1.release()
    else {
      val votersNormalPriority = voters.filter(_._2 == Priority.Normal)
      val newLeader = if (votersNormalPriority.nonEmpty)
        randomFunction(votersNormalPriority.map(_._1)) else randomFunction(voters.map(_._1))

      chooseLeader(currentLeader._1, newLeader)
    }
  }

}

object LeaderSelectorPriority extends App {
  object Priority extends Enumeration {
    val Normal, Low = Value
  }

  def getRandomLeader(voters: ArrayBuffer[MyLeaderSelectorClient]): MyLeaderSelectorClient = {
    if (voters.nonEmpty) voters(scala.util.Random.nextInt(voters.length))
    else throw new Exception("Bug!")
  }
}
