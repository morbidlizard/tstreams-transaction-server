package zooKeeper

import scala.annotation.tailrec
import scala.collection.mutable

trait Tree {
  def addAgent(agent: Agent, streamName: String, partitions: Seq[Int])
}

class ZooTree(connectionString: String) extends Tree
{
  val streams: mutable.ArrayBuffer[StreamLeaderSelectorPriority] = new mutable.ArrayBuffer[StreamLeaderSelectorPriority]()

  private def streamNameToStreamPath(streamName: String) = s"/$streamName"

  def addStream(name: String, partitionNumber: Int) = this.synchronized {
    val stream = new StreamLeaderSelectorPriority(connectionString, streamNameToStreamPath(name), partitionNumber)
    streams += stream
    stream
  }

  def getStreamPartitionLeader(streamName: String, partition: Int) = {
    val streamOpt = streams.find(_.streamPath == s"/$streamName")
    streamOpt match {
      case Some(stream) => stream.partitions(partition).getLeaderId
      case None => None
    }
  }

  private def intToZooId(partition: Int): String = {
    // From 1 to 10 is length of Id in Zookeeper that CreateMode.ETHERMAL_SEQ has.
    val length = 10
    @tailrec def zeroesForPath(n: Int, acc: String): String = if (n>0) zeroesForPath(n - 1, "0" + acc) else acc
    val str = partition.toString
    zeroesForPath(length - str.length, str)
  }

  def addAgent(agent: Agent, streamName: String, partitions: Seq[Int]) = this.synchronized {
    val streamOpt = streams.find(_.streamPath == streamNameToStreamPath(streamName))
    streamOpt match {
      case None => throw new NoSuchElementException("Stream doesn't exist!")
      case Some(stream) => {
        if (partitions.max > stream.partitions.length)
          throw new NoSuchElementException(s"Stream $streamName doesn't contain all of that $partitions!")
        else partitions foreach (partition => stream.addAgentToPartition(intToZooId(partition), agent))
      }
    }
  }

  def deleteStream(streamName: String):Unit = this.synchronized {
    streams.find(_.streamPath == streamNameToStreamPath(streamName)) foreach {stream =>
      streams -= stream
      stream.close()
    }
  }

  def closeAgent(agent:Agent): Unit = streams.foreach(stream=> stream.deleteAndCloseAgent(agent))

  def close() = streams.foreach(_.close())
}
