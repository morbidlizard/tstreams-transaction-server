package com.bwsw.tstreamstransactionserver.netty.server.db

import java.util.concurrent.atomic.AtomicLong

abstract class KeyValueDatabaseBatch(idGenerator: AtomicLong) {
  val id: Long = idGenerator.incrementAndGet()

  def put(index: Int, key: Array[Byte], data: Array[Byte]): Boolean

  def remove(index: Int, key: Array[Byte]): Unit

  def write(): Boolean
}
