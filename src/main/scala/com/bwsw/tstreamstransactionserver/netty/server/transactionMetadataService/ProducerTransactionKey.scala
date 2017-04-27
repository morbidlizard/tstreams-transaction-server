package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionKey.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class ProducerTransactionKey(stream: Long, partition: Int, transactionID: Long) extends Ordered[ProducerTransactionKey]{
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }

  override def compare(that: ProducerTransactionKey): Int = {
    if (this.stream < that.stream) -1
    else if (this.stream > that.stream) 1
    else if (this.partition < that.partition) -1
    else if (this.partition > that.partition) 1
    else if (this.transactionID < that.transactionID) -1
    else if (this.transactionID > that.transactionID) 1
    else 0
  }

  def toByteArray: Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(
      java.lang.Long.BYTES +
      java.lang.Integer.BYTES +
      java.lang.Long.BYTES
    )
    buffer
      .putLong(stream)
      .putInt(partition)
      .putLong(transactionID)
      .array()
  }
}

object ProducerTransactionKey extends TupleBinding[ProducerTransactionKey] {
  override def entryToObject(input: TupleInput): ProducerTransactionKey = {
    val stream = input.readLong()
    val partition = input.readInt()
    val transactionID = input.readLong()
    ProducerTransactionKey(stream, partition, transactionID)
  }

  override def objectToEntry(key: ProducerTransactionKey, output: TupleOutput): Unit = {
    output.writeLong(key.stream)
    output.writeInt(key.partition)
    output.writeLong(key.transactionID)
  }

  def fromByteArray(bytes: Array[Byte]): ProducerTransactionKey = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val stream = buffer.getLong
    val partition = buffer.getInt
    val transactionID = buffer.getLong
    ProducerTransactionKey(stream, partition, transactionID)
  }
}




