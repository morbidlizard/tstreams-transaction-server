package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamKey.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

class StreamKey(val id: Long) extends AnyVal {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
      .putLong(id)
      .array()
  }
}

object StreamKey extends TupleBinding[StreamKey] {
  def apply(id: Long): StreamKey = new StreamKey(id)

  override def entryToObject(input: TupleInput): StreamKey = StreamKey(input.readLong())
  override def objectToEntry(key: StreamKey, output: TupleOutput): Unit = output.writeLong(key.id)

  def fromByteArray(bytes: Array[Byte]) : StreamKey = {
    val id = java.nio.ByteBuffer.wrap(bytes).getLong
    StreamKey(id)
  }
}
