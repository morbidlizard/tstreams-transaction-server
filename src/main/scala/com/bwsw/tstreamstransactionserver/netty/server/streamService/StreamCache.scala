package com.bwsw.tstreamstransactionserver.netty.server.streamService

trait StreamCache {
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): StreamKey
  def checkStreamExists(streamID: Int): Boolean
  def getStream(streamID: Int): Option[StreamRecord]
  def getAllStreams: Seq[StreamRecord]
  def delStream(streamID: Int): Boolean
}
