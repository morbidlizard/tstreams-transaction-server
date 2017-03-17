package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry
import Key.objectToEntry

case class Key(stream: Long, partition: Int, transactionID: Long) extends Ordered[Key]{
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }

  override def compare(that: Key): Int = {
    if (this.stream < that.stream) -1
    else if (this.stream > that.stream) 1
    else if (this.partition < that.partition) -1
    else if (this.partition > that.partition) 1
    else if (this.transactionID < that.transactionID) -1
    else if (this.transactionID > that.transactionID) 1
    else 0
  }
  override def toString: String = s"stream:$stream\tpartition:$partition\tid:$transactionID"
}

object Key extends TupleBinding[Key] {
  override def entryToObject(input: TupleInput): Key = {
    val stream = input.readLong()
    val partition = input.readInt()
    val transactionID = input.readLong()
    Key(stream, partition, transactionID)
  }

  override def objectToEntry(key: Key, output: TupleOutput): Unit = {
    output.writeLong(key.stream)
    output.writeInt(key.partition)
    output.writeLong(key.transactionID)
  }
}




