package com.bwsw.tstreamstransactionserver.netty.server.db.zk

import com.bwsw.tstreamstransactionserver.netty.server.streamService
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamRecord}
import com.bwsw.tstreamstransactionserver.rpc.StreamValue
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
//import org.slf4j.LoggerFactory

final class StreamIDPath(client: CuratorFramework, path: String) {
//  private val logger = LoggerFactory.getLogger(this.getClass)

  private val seqPrefix = "id"
  private val streamsIdsPath = s"$path/$seqPrefix"

  private def buildPath(streamID: Int): String = f"$streamsIdsPath$streamID%010d"

  private def getIDFromPath(pathWithId: String): String = pathWithId.splitAt(
    path.length + seqPrefix.length + 1
  )._2


  def put(streamValue: streamService.StreamValue): streamService.StreamRecord = {
    val id = client.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(streamsIdsPath, Array.emptyByteArray)

    val streamKey = StreamKey(getIDFromPath(id).toInt)

    val streamValueWithPath: streamService.StreamValue =
      streamService.StreamValue(streamValue.name,
        streamValue.partitions,
        streamValue.description,
        streamValue.ttl,
        Some(id)
      )

    val streamRecord = StreamRecord(streamKey, streamValueWithPath)

    client.setData().forPath(id, streamRecord.toBinaryJson)

    streamRecord
  }

  def checkExists(streamKey: streamService.StreamKey): Boolean = {
    Option(client.checkExists().forPath(buildPath(streamKey.id)))
      .exists(_ => true)
  }

  def get(streamKey: streamService.StreamKey): Option[streamService.StreamRecord] = {
    val streamValueOpt = scala.util.Try(client.getData.forPath(buildPath(streamKey.id)))
    val streamRecord   = streamValueOpt.toOption
      .map(data => StreamRecord.fromBinaryJson(data))

    streamRecord
  }

//  def delete(streamKey: StreamKey, streamPath: StreamNamePath): Boolean = {
//    if (checkExists(streamKey)) {
//      val isDeleted = Option(client.getData.forPath(buildPath(streamKey.id)))
//        .map(data => StreamRecord.fromBinaryJson(data))
//        .map(streamRecord => streamPath.delete(streamRecord.name, this))
//        .map(_ => client.setData().forPath(buildPath(streamKey.id), Array.emptyByteArray))
//        .exists(_ => true)
//
//      if (isDeleted && logger.isDebugEnabled) {
//        logger.debug(s"Stream with id ${streamKey.id} is ${if (isDeleted) "" else "not"} deleted")
//      }
//      isDeleted
//    }
//    else {
//      false
//    }
//  }
}

