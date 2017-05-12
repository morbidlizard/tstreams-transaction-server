package com.bwsw.tstreamstransactionserver.netty.server.db.zk

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamRecord, StreamValue}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

final class StreamNamePath(client: CuratorFramework, path: String) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def put(streamRecord: StreamRecord): Unit = {
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


  def checkExists(streamName: String): Boolean = {
    Option(client.checkExists().forPath(s"$path/$streamName"))
      .exists(_ => true)
  }

  def get(streamName: String): Option[StreamRecord] = {
    scala.util.Try(client.getData.forPath(s"$path/$streamName"))
      .toOption.map(data => StreamRecord.fromBinaryJson(data))
  }

  def delete(streamName: String): Boolean = {
    val isDeleted = scala.util.Try(client.delete().forPath(s"$path/$streamName"))
      .isSuccess

    if (isDeleted && logger.isDebugEnabled) {
      logger.debug(s"Stream name $streamName is ${if (isDeleted) "" else "not"} deleted")
    }

    isDeleted
  }

}