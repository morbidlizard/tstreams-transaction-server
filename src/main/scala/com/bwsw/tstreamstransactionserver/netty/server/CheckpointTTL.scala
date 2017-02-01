package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.server.streamService.KeyStream

trait CheckpointTTL {
  val streamTTL = new java.util.concurrent.ConcurrentHashMap[String, KeyStream]()
  def getStreamDatabaseObject(stream: String): KeyStream
}
private object CheckpointTTL {
  val streamTTL = new java.util.concurrent.ConcurrentHashMap[String, KeyStream]()
}
