package com.bwsw.tstreamstransactionserver.netty.server.db.zk

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCRUD, StreamKey, StreamRecord, StreamValue}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

final class StreamDatabaseZK(client: CuratorFramework, path: String) extends StreamCRUD {
  private val streamNamePath = new StreamNamePath(client, s"$path/names")
  private val streamIDPath = new streamIDPath(client, s"$path/ids")

  override def putStream(streamValue: StreamValue): StreamKey = {
    val streamRecord = streamIDPath.put(streamValue)
    streamNamePath.put(streamRecord)
    streamRecord.key
  }

  override def checkStreamExists(name: String): Boolean = streamNamePath.checkExists(name)

  override def delStream(name: String): Boolean = streamNamePath.delete(name, streamIDPath)

  override def getStream(name: String): Option[StreamRecord] = streamNamePath.get(name)

  override def getStream(streamKey: StreamKey): Option[StreamRecord] = streamIDPath.get(streamKey, streamNamePath)
}

