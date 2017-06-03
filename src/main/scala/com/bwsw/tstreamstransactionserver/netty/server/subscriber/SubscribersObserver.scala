package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import java.util
import java.util.concurrent.{Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCRUD, StreamKey}
import org.apache.curator.framework.CuratorFramework
import org.slf4j.{Logger, LoggerFactory}


private object SubscribersObserver {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
}


private[subscriber] final class SubscribersObserver(curatorClient: CuratorFramework,
                                streamInteractor: StreamCRUD,
                                updatePeriodMs: Int)
{
  @volatile private var isStopped = false

  private val scheduledExecutor = Executors
    .newSingleThreadScheduledExecutor()

  private val partitionSubscribers =
    new java.util.concurrent.ConcurrentHashMap[StreamPartitionUnit, java.util.List[String]]()

  private val updateSubscribersTask = new Runnable {
    override def run(): Unit = {
      val keys = partitionSubscribers.keys()
      while (keys.hasMoreElements) {
        val streamPartition = keys.nextElement()
        updateSubscribers(
          streamPartition.streamID,
          streamPartition.partition
        )
      }
    }
  }

  private def runSubscriberUpdateTask(): Unit = {
    scheduledExecutor.scheduleWithFixedDelay(
      updateSubscribersTask,
      0L,
      updatePeriodMs,
      TimeUnit.MILLISECONDS
    )
  }

  //Notify subscribers every 1 sec
  runSubscriberUpdateTask()


  def addSteamPartition(stream: Int, partition: Int): Unit = {
    updateSubscribers(stream, partition)
  }

  def getStreamPartitionSubscribers(stream: Int, partition: Int): Option[util.List[String]] = {
    val streamPartition = StreamPartitionUnit(stream, partition)
    Option(partitionSubscribers.get(streamPartition))
  }


  /**
    * Update subscribers on specific partition
    */
  private def updateSubscribers(stream: Int, partition: Int): Unit = {
    val streamPartition = StreamPartitionUnit(stream, partition)
    streamInteractor.getStream(StreamKey(streamPartition.streamID)).foreach {
      stream =>
        scala.util.Try(
          curatorClient.getChildren
            .forPath(s"${stream.zkPath}/subscribers/${streamPartition.partition}")
        ).map { childrenPaths =>
          if (childrenPaths.isEmpty)
            partitionSubscribers.remove(streamPartition)
          else
            partitionSubscribers.put(streamPartition, childrenPaths)
        }
    }
  }

  /**
    * Stop this Subscriber client
    */
  def shutdown(): Unit = {
    if (isStopped) {
      throw new IllegalStateException(
        "Producer->Subscriber notifier was stopped second time."
      )
    } else {
      isStopped = true

      scheduledExecutor.shutdown()
      scala.util.Try(scheduledExecutor.awaitTermination(
        updatePeriodMs,
        TimeUnit.MILLISECONDS
      ))
      partitionSubscribers.clear()
    }
  }
}

