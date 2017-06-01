package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import java.util
import java.util.concurrent.{Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCRUD, StreamKey}
import org.apache.curator.framework.CuratorFramework
import org.slf4j.{Logger, LoggerFactory}


private object StreamPartitionClientsObserver {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
}


final class StreamPartitionClientsObserver(curatorClient: CuratorFramework,
                                           streamCRUD: StreamCRUD,
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
        updateSubscribers(keys.nextElement())
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


  def addSteamPartition(streamPartition: StreamPartitionUnit): Unit =
    updateSubscribers(streamPartition)

  def getStreamPartitionSubscribers(streamPartition: StreamPartitionUnit): Option[util.List[String]] = {
    Option(partitionSubscribers.get(streamPartition))
  }


  /**
    * Update subscribers on specific partition
    */
  private def updateSubscribers(streamPartition: StreamPartitionUnit): Unit = {
    streamCRUD.getStream(StreamKey(streamPartition.streamID)).foreach {
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
  def stop(): Unit = {
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

