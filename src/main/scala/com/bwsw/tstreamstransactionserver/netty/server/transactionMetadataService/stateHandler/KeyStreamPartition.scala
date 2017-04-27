package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class KeyStreamPartition(stream: Long, partition: Int) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    KeyStreamPartition.objectToEntry(this, databaseEntry)
    databaseEntry
  }
  def toByteArray: Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(
      java.lang.Long.BYTES + java.lang.Integer.BYTES
    )
    buffer
      .putLong(stream)
      .putInt(partition)
      .array()
  }
}

object KeyStreamPartition extends TupleBinding[KeyStreamPartition] {
  override def entryToObject(input: TupleInput): KeyStreamPartition = KeyStreamPartition(input.readLong(), input.readInt())
  override def objectToEntry(key: KeyStreamPartition, output: TupleOutput): Unit = {
    output.writeLong(key.stream)
    output.writeInt(key.partition)
  }
  def fromByteArray(bytes: Array[Byte]): KeyStreamPartition = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val stream = buffer.getLong
    val partition = buffer.getInt
    KeyStreamPartition(stream, partition)
  }
}
