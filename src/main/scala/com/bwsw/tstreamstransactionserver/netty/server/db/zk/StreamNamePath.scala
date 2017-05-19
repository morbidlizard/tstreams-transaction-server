package com.bwsw.tstreamstransactionserver.netty.server.db.zk

import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamRecord
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.framework.state.ConnectionState
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

final class StreamNamePath(client: CuratorFramework, path: String) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val semaphore = new InterProcessSemaphoreMutex(client, path)
  private def lock[T](body: => T) = {
    semaphore.acquire()
    val result = body
    scala.util.Try(semaphore.release())
    result
  }

  client.getConnectionStateListenable.addListener(
    (_: CuratorFramework, newState: ConnectionState) => newState match {
      case ConnectionState.SUSPENDED => scala.util.Try(semaphore.release())
      case ConnectionState.LOST      => scala.util.Try(semaphore.release())
      case _ =>
    })

  def put(streamRecord: StreamRecord): Unit =
    lock {
      client.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(
          s"$path/${streamRecord.name}",
          streamRecord.toBinaryJson
        )

      if (logger.isDebugEnabled)
        logger.debug(s"Persisted stream: " +
          s"id ${streamRecord.id}, " +
          s"name ${streamRecord.name}, " +
          s"partitions ${streamRecord.partitions}, " +
          s"description ${streamRecord.description}, " +
          s"ttl ${streamRecord.ttl}"
        )
    }


  def checkExists(streamName: String): Boolean =
    lock {
      Option(client.checkExists().forPath(s"$path/$streamName"))
        .exists(_ => true)
    }

  def get(streamName: String): Option[StreamRecord] =
    lock {
      scala.util.Try(client.getData.forPath(s"$path/$streamName"))
        .toOption.map(data => StreamRecord.fromBinaryJson(data))
    }

  def delete(streamName: String): Boolean =
    lock {
      val isDeleted = scala.util.Try(client.delete().forPath(s"$path/$streamName"))
        .isSuccess

      if (isDeleted && logger.isDebugEnabled) {
        logger.debug(s"Stream name $streamName is ${if (isDeleted) "" else "not"} deleted")
      }

      isDeleted
    }
}