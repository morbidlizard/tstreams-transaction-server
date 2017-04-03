package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamKey.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class StreamKey(streamNameAsLong: Long) extends AnyVal {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object StreamKey extends TupleBinding[StreamKey] {
  override def entryToObject(input: TupleInput): StreamKey = StreamKey(input.readLong())
  override def objectToEntry(key: StreamKey, output: TupleOutput): Unit = output.writeLong(key.streamNameAsLong)
}
