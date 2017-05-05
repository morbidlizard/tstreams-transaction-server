package com.bwsw.tstreamstransactionserver.netty.server.db.zk

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCache, StreamKey, StreamRecord, StreamValue}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

final class StreamDatabaseZK(client: CuratorFramework, path: String) extends StreamCache {
  import scala.collection.JavaConverters._
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val streamCache = new java.util.concurrent.ConcurrentHashMap[StreamKey, StreamValue]()

  private val seqPrefix = "id"
  private val streamsPath = s"$path/$seqPrefix"
  private def buildPath(streamID: Int): String = f"$streamsPath$streamID%010d"
  private def getIDFromPath(pathWithId: String): String = pathWithId.splitAt(path.length + seqPrefix.length + 1)._2
  private def getID(id: String): String = id.splitAt(seqPrefix.length + 1)._2

  private def initializeCache(): Unit = {
    val statOpt = Option(client.checkExists().creatingParentContainersIfNeeded().forPath(path))
    val streams = statOpt.map{_ =>
      val streamsIds = client.getChildren.forPath(path).asScala
      streamsIds.map{id =>
        val streamValueBinary = client.getData.forPath(path + '/' + id)
        val streamKey   = StreamKey(getID(id).toInt)
        val streamValue = StreamValue.fromByteArray(streamValueBinary)
        if (logger.isDebugEnabled)
          logger.debug(s"Loading stream to cache: " +
            s"id ${streamKey.id}, " +
            s"name ${streamValue.name}, " +
            s"partitions ${streamValue.partitions}, " +
            s"description ${streamValue.description}" +
            s"ttl ${streamValue.ttl}"
          )
        StreamRecord(streamKey, streamValue)
      }
    }.getOrElse{
      if (logger.isDebugEnabled)
        logger.debug(s"There are no streams on path $path, may be it's first run of TTS.")
      Seq.empty[StreamRecord]
    }

    streams.foreach(record => streamCache.put(record.key, record.stream))
  }

  override def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): StreamKey = {
    val streamValue = StreamValue(stream, partitions, description, ttl)

    val id = client.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(streamsPath, streamValue.toByteArray)
    val streamKey = StreamKey(getIDFromPath(id).toInt)
    if (logger.isDebugEnabled)
      logger.debug(s"Persisted stream: " +
        s"id ${streamKey.id}, " +
        s"name ${streamValue.name}, " +
        s"partitions ${streamValue.partitions}, " +
        s"description ${streamValue.description}" +
        s"ttl ${streamValue.ttl}"
      )
    StreamRecord(streamKey, streamValue)
    streamCache.put(streamKey, streamValue)
    streamKey
  }

  override def checkStreamExists(streamID: Int): Boolean = {
    val streamKey = StreamKey(streamID)
    val doesExist = Option(streamCache.get(streamKey))
      .orElse(Option(client.checkExists().forPath(buildPath(streamID))))
      .exists(_ => true)
    if (logger.isDebugEnabled)
      logger.debug(s"Stream with id $streamID ${if (doesExist) "exists" else "doesn't exist"}")
    doesExist
  }

  override def delStream(streamID: Int): Boolean = {
    if (checkStreamExists(streamID)) {
      client.delete().forPath(buildPath(streamID))
      streamCache.remove( StreamKey(streamID))
      if (logger.isDebugEnabled)
        logger.debug(s"Stream with id $streamID is deleted")
      true
    }
    else false
  }

  override def getStream(streamID: Int): Option[StreamRecord] = {
    val streamKey = StreamKey(streamID)
    Option(streamCache.get(streamKey))
      .map(streamValue => StreamRecord(streamKey, streamValue))
      .orElse {
        val streamValueOpt = scala.util.Try(client.getData.forPath(buildPath(streamID)))
        streamValueOpt.toOption.map { streamValueBinary =>
          val streamValue = StreamValue.fromByteArray(streamValueBinary)
          streamCache.put(streamKey, streamValue)
          StreamRecord(streamKey, streamValue)
        }
      }
  }

  override def getAllStreams: Seq[StreamRecord] = {
    streamCache.asScala.map{case (key,value) => StreamRecord(key,value)}.toSeq
  }
  initializeCache()
}

