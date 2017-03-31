package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.streamService.KeyStream
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

trait StreamCache {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private[server] val streamCache = new java.util.concurrent.ConcurrentHashMap[String, ArrayBuffer[KeyStream]]()
  protected[server] def getStreamFromOldestToNewest(stream: String): ArrayBuffer[KeyStream]

  @throws[StreamDoesNotExist]
  final def getMostRecentStream(stream: String): KeyStream = {
    val streams = getStreamFromOldestToNewest(stream)
    val recentNotDeletedStreamOpt = streams.lastOption
    recentNotDeletedStreamOpt match {
      case Some(streamObj) if !streamObj.stream.deleted => streamObj
      case _ =>
        val streamDoesntExistThrowable = new StreamDoesNotExist(stream)
        if (logger.isDebugEnabled()) logger.debug(streamDoesntExistThrowable.getMessage)
        throw streamDoesntExistThrowable
    }
  }

  final def getStreamObjByID(stream: Long): KeyStream = {
    import scala.collection.JavaConverters._
    val recentNotDeletedStreamOpt = streamCache.values().asScala.find(x => x.lastOption.get.streamNameToLong == stream)
    recentNotDeletedStreamOpt match {
      case Some(streamObj) if !streamObj.last.stream.deleted => streamObj.last
      case _ =>
        val streamDoesntExistThrowable = new StreamDoesNotExist(stream.toLong.toString)
        if (logger.isDebugEnabled()) logger.debug(streamDoesntExistThrowable.getMessage)
        throw streamDoesntExistThrowable
    }
  }
}
