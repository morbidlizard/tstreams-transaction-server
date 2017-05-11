package com.bwsw.tstreamstransactionserver.netty.server.streamService

trait StreamCache {
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): StreamKey
  def checkStreamExists(streamKey: StreamKey): Boolean
  def getStream(streamKey: StreamKey): Option[StreamRecord]
  def getAllStreams: Seq[StreamRecord]
  def delStream(streamKey: StreamKey): Boolean
}
