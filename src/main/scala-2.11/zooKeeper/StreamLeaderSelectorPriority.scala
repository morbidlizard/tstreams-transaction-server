package zooKeeper

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import scala.language.implicitConversions

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class StreamLeaderSelectorPriority(connectionString: String, val streamPath: String, val partitionNumber: Int) {
  import StreamLeaderSelectorPriority._
  implicit private def agentPriorityToLeaderSelectorPriority(priority: Agent.Priority.Value) : LeaderSelectorPriority.Priority.Value = priority match {
    case Agent.Priority.Normal => LeaderSelectorPriority.Priority.Normal
    case Agent.Priority.Low => LeaderSelectorPriority.Priority.Low
  }

  private def newConnectionClient = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))
  val client = {
    val clt = newConnectionClient
    clt.start()
    clt.getZookeeperClient.blockUntilConnectedOrTimedOut()
    clt
  }


  createPathIfItNotExists(streamPath)
  private def createPathIfItNotExists(path: String):String = {
    val pathOpt = Option(client.checkExists().forPath(path))
    if (pathOpt.isDefined) path
    else client.create()
      .creatingParentContainersIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .forPath(path)
  }

  val partitions: Array[LeaderSelectorPriority] = Array.fill(partitionNumber)(addPartition())
  def addPartition(): LeaderSelectorPriority = {
    val partitionId = client.create
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(s"$streamPath/",Array[Byte]())
    new LeaderSelectorPriority(partitionId)
  }

  private val partitionAgents = scala.collection.concurrent.TrieMap[String, mutable.ArrayBuffer[Agent]]()
  def addAgentToPartition(partitionId: String, agent: Agent): Unit = {
    val client = if (connectionPerAgent.isDefinedAt(agent)) connectionPerAgent(agent)
    else {
      val clnt = newConnectionClient
      connectionPerAgent.getOrElseUpdate(agent, clnt)
      clnt.start()
      clnt.getZookeeperClient.blockUntilConnectedOrTimedOut()
      clnt
    }

    val leaderSelectorOfPartition = partitions(partitionId.toInt)
    leaderSelectorOfPartition addId(agent.toString, agent.priority, client)

    if (partitionAgents.isDefinedAt(partitionId))
      partitionAgents(partitionId) += agent
    else
      partitionAgents += ((partitionId, ArrayBuffer(agent)))
  }

  def deleteAndCloseAgent(agent: Agent):Unit = {
    partitions.foreach(leaderSelectorPriority => leaderSelectorPriority.removeId(agent.toString))
    partitionAgents foreach {case (_,agents) => agents -= agent}
  }

  def close() = {
    //partitions foreach (_.close())
    client.close()
  }
}

private object StreamLeaderSelectorPriority {
  import org.apache.curator.framework.CuratorFramework
  val connectionPerAgent = scala.collection.concurrent.TrieMap[Agent, CuratorFramework]()
}

