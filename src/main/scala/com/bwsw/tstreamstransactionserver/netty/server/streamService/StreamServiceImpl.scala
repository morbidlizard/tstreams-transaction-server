package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.rpc.Stream


class StreamServiceImpl(streamCache: StreamCRUD)
{
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): Int =
    this.synchronized {
      streamCache.putStream(StreamValue(stream, partitions, description, ttl)).id
    }

  def getStream(name: String): Option[Stream] =
      streamCache.getStream(name)

  def delStream(name: String): Boolean =
    this.synchronized {
      streamCache.delStream(name)
    }

  def checkStreamExists(name: String): Boolean =
    streamCache.checkStreamExists(name)

}