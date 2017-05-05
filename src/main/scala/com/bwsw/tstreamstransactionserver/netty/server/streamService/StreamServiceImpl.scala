package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.rpc.Stream

class StreamServiceImpl(streamCache: StreamCache)
{
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): Int =
    streamCache.putStream(stream, partitions, description, ttl).id

  def getStream(streamID: Int): Stream = {
    streamCache.getStream(streamID).getOrElse(throw new StreamDoesNotExist(streamID.toString))
  }

  def delStream(streamID: Int): Boolean = streamCache.delStream(streamID)

  def checkStreamExists(streamID: Int): Boolean = streamCache.checkStreamExists(streamID)

  def getAllStreams: Seq[Stream] = streamCache.getAllStreams
}