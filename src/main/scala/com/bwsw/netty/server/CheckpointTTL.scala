package com.bwsw.netty.server

trait CheckpointTTL {
  val streamTTL = new java.util.concurrent.ConcurrentHashMap[String, com.bwsw.netty.server.streamService.KeyStream]()
  def getStreamDatabaseObject(stream: String): com.bwsw.netty.server.streamService.KeyStream
}
private object CheckpointTTL {
  val streamTTL = new java.util.concurrent.ConcurrentHashMap[String, com.bwsw.netty.server.streamService.KeyStream]()
}
