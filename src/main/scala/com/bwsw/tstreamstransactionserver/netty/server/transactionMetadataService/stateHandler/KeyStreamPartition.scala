package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class KeyStreamPartition(stream: Long, partition: Int) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    KeyStreamPartition.objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object KeyStreamPartition extends TupleBinding[KeyStreamPartition] {
  override def entryToObject(input: TupleInput): KeyStreamPartition = KeyStreamPartition(input.readLong(), input.readInt())
  override def objectToEntry(key: KeyStreamPartition, output: TupleOutput): Unit = {
    output.writeLong(key.stream)
    output.writeInt(key.partition)
  }
}
