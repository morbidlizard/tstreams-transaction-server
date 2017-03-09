package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class Key(stream: String, partition: Int) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    Key.objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object Key extends TupleBinding[Key] {
  override def entryToObject(input: TupleInput): Key = Key(input.readString(), input.readInt())
  override def objectToEntry(key: Key, output: TupleOutput): Unit = {
    output.writeString(key.stream)
    output.writeInt(key.partition)
  }
}
