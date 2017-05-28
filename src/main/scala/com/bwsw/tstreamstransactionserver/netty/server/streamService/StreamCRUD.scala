package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.netty.server.streamService

trait StreamCRUD {
  def putStream(streamValue: streamService.StreamValue): streamService.StreamKey
  def checkStreamExists(name: String): Boolean
  def getStream(streamKey: streamService.StreamKey): Option[streamService.StreamRecord]
  def getStream(name: String): Option[streamService.StreamRecord]
  def delStream(name: String): Boolean
}
