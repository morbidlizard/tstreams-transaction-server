package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.server.streamService.KeyStream

trait StreamCache {
  val streamCache = new java.util.concurrent.ConcurrentHashMap[String, KeyStream]()
  def getStreamDatabaseObject(stream: String): KeyStream
}
