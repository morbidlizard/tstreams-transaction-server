package com.bwsw.tstreamstransactionserver.netty.server.streamService


class StreamServiceImpl(streamCache: StreamCRUD)
{
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): Int =
    streamCache.putStream(StreamValue(stream, partitions, description, ttl, None)).id

  def getStream(name: String): Option[StreamRecord] =
    streamCache.getStream(name)

  def delStream(name: String): Boolean =
    streamCache.delStream(name)

  def checkStreamExists(name: String): Boolean =
    streamCache.checkStreamExists(name)
}