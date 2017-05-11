package com.bwsw.tstreamstransactionserver.netty.server.db.zk

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCache, StreamKey, StreamRecord, StreamValue}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

final class StreamDatabaseZK(client: CuratorFramework, path: String) extends StreamCache {

  import scala.collection.JavaConverters._

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val seqPrefix = "id"
  private val streamsPath = s"$path/$seqPrefix"

  private def buildPath(streamID: Int): String = f"$streamsPath$streamID%010d"

  private def getIDFromPath(pathWithId: String): String = pathWithId.splitAt(path.length + seqPrefix.length + 1)._2

  private def getID(id: String): String = id.splitAt(seqPrefix.length + 1)._2

  private def initializeCache(): ConcurrentHashMap[StreamKey, StreamValue] = {
    val statOpt = Option(client.checkExists().creatingParentContainersIfNeeded().forPath(path))
    val streamCache = new java.util.concurrent.ConcurrentHashMap[StreamKey, StreamValue]()
    statOpt.foreach { _ =>
      val streamsIds = client.getChildren.forPath(path).asScala
      streamsIds.foldLeft(streamCache) { (cache, id) =>
        val streamValueBinary = client.getData.forPath(path + '/' + id)
        if (streamValueBinary.nonEmpty) {
          val streamKey = StreamKey(getID(id).toInt)
          val streamValue = StreamValue.fromBinaryJson(streamValueBinary)
          if (logger.isDebugEnabled)
            logger.debug(s"Loading stream to cache: " +
              s"id ${streamKey.id}, " +
              s"name ${streamValue.name}, " +
              s"partitions ${streamValue.partitions}, " +
              s"description ${streamValue.description}" +
              s"ttl ${streamValue.ttl}"
            )
          cache.put(streamKey, streamValue)
          cache
        } else {
          cache
        }
      }
    }

    if (streamCache.isEmpty) {
      if (logger.isDebugEnabled)
        logger.debug(s"There are no streams on path $path, may be it's first run of TTS.")
    }
    streamCache
  }

  private val streamCache = initializeCache()

  override def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): StreamKey = {
    val streamValue = StreamValue(stream, partitions, description, ttl)

    val id = client.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(streamsPath, streamValue.toBinaryJson)

    val streamKey = StreamKey(getIDFromPath(id).toInt)
    if (logger.isDebugEnabled)
      logger.debug(s"Persisted stream: " +
        s"id ${streamKey.id}, " +
        s"name ${streamValue.name}, " +
        s"partitions ${streamValue.partitions}, " +
        s"description ${streamValue.description}" +
        s"ttl ${streamValue.ttl}"
      )
    streamCache.put(streamKey, streamValue)
    streamKey
  }

  override def checkStreamExists(streamKey: StreamKey): Boolean = {
    val empty = 0
    val doesExist =
      (if (streamCache.contains(streamKey)) Some(true) else None)
        .orElse(
          Option(
            client.checkExists().forPath(buildPath(streamKey.id))
          ).map(stat => stat.getDataLength != empty)
        )
        .getOrElse(false)

    if (logger.isDebugEnabled)
      logger.debug(s"Stream with id ${streamKey.id} ${if (doesExist) "exists" else "doesn't exist"}")

    doesExist
  }

  override def delStream(streamKey: StreamKey): Boolean = {
    if (checkStreamExists(streamKey)) {
      client.setData().forPath(buildPath(streamKey.id), Array.emptyByteArray)
      streamCache.remove(streamKey)
      if (logger.isDebugEnabled)
        logger.debug(s"Stream with id ${streamKey.id} is deleted")
      true
    }
    else {
      false
    }
  }

  override def getStream(streamKey: StreamKey): Option[StreamRecord] = {
    val stream = Option(streamCache.get(streamKey))
      .map(streamValue => StreamRecord(streamKey, streamValue))
      .orElse {
        val streamValueOpt = scala.util.Try(client.getData.forPath(buildPath(streamKey.id)))
        streamValueOpt.toOption.flatMap { streamValueBinary =>
          if (streamValueBinary.nonEmpty) {
            val streamValue = StreamValue.fromBinaryJson(streamValueBinary)
            streamCache.put(streamKey, streamValue)
            Some(StreamRecord(streamKey, streamValue))
          } else {
            None
          }
        }
      }
    stream
  }

  override def getAllStreams: Seq[StreamRecord] = {
    streamCache.asScala.map { case (key, value) => StreamRecord(key, value) }.toSeq
  }

  initializeCache()
}

