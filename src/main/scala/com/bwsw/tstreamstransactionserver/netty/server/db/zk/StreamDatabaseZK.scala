package com.bwsw.tstreamstransactionserver.netty.server.db.zk


import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCRUD}
import com.bwsw.tstreamstransactionserver.netty.server.streamService
import org.apache.curator.framework.CuratorFramework

final class StreamDatabaseZK(client: CuratorFramework, path: String)
  extends StreamCRUD
{
  private val streamCache =
    new ConcurrentHashMap[streamService.StreamKey, streamService.StreamValue]()

  private val streamNamePath = new StreamNamePath(client, s"$path/names")
  private val streamIDPath   = new StreamIDPath(client, s"$path/ids")

  override def putStream(streamValue: streamService.StreamValue): streamService.StreamKey = {
    if (!streamNamePath.checkExists(streamValue.name)) {
      val streamRecord = streamIDPath.put(streamValue)
      streamNamePath.put(streamRecord)
      streamCache.put(streamRecord.key, streamRecord.stream)
      streamRecord.key
    } else streamService.StreamKey(-1)
  }

  override def checkStreamExists(name: String): Boolean =
    streamNamePath.checkExists(name)


  override def delStream(name: String): Boolean =
    streamNamePath.delete(name)


  override def getStream(name: String): Option[streamService.StreamRecord] =
    streamNamePath.get(name)


  override def getStream(streamKey: streamService.StreamKey): Option[streamService.StreamRecord] = {
    Option(streamCache.get(streamKey))
      .map(steamValue => streamService.StreamRecord(streamKey, steamValue))
      .orElse{
        val streamRecordOpt = streamIDPath.get(streamKey)
        streamRecordOpt.foreach(streamRecord =>
          streamCache.put(streamRecord.key, streamRecord.stream)
        )
        streamRecordOpt
      }
  }
}

