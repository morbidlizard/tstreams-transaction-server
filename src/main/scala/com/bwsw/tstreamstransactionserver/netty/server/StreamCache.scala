package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.server.streamService.KeyStream

import scala.collection.mutable.ArrayBuffer

trait StreamCache {
  val streamCache = new java.util.concurrent.ConcurrentHashMap[String, ArrayBuffer[KeyStream]]()
  def getStreamFromOldestToNewest(stream: String): ArrayBuffer[KeyStream]
}
