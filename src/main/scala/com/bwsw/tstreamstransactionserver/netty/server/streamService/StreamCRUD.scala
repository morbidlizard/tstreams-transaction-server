package com.bwsw.tstreamstransactionserver.netty.server.streamService

trait StreamCRUD {
  def putStream(streamValue: StreamValue): StreamKey
  def checkStreamExists(name: String): Boolean
  def getStream(streamKey: StreamKey): Option[StreamRecord]
  def getStream(name: String): Option[StreamRecord]
  def delStream(name: String): Boolean
}
