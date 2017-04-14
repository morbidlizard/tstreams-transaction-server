package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionKey.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class ConsumerTransactionKey(name: String, stream: java.lang.Long, partition: java.lang.Integer) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
  override def toString: String = s"Consumer transaction key: name:$name stream:$stream partition:$partition"
}

object ConsumerTransactionKey extends TupleBinding[ConsumerTransactionKey] {
  override def entryToObject(input: TupleInput): ConsumerTransactionKey = {
    val name = input.readString()
    val stream = input.readLong()
    val partition = input.readInt()
    ConsumerTransactionKey(name, stream, partition)
  }

  override def objectToEntry(key: ConsumerTransactionKey, output: TupleOutput): Unit = {
    output.writeString(key.name)
    output.writeLong(key.stream)
    output.writeInt(key.partition)
  }
}
