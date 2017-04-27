package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionValue.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry


case class ConsumerTransactionValue(transactionId: Long, timestamp:Long) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer
      .allocate(java.lang.Long.BYTES + java.lang.Long.BYTES)
      .putLong(transactionId)
      .putLong(timestamp)
      .array()
  }
}

object ConsumerTransactionValue extends TupleBinding[ConsumerTransactionValue] {
  override def entryToObject(input: TupleInput): ConsumerTransactionValue = ConsumerTransactionValue(input.readLong(), input.readLong())
  override def objectToEntry(consumerTransaction: ConsumerTransactionValue, output: TupleOutput): Unit = {
    output.writeLong(consumerTransaction.transactionId)
    output.writeLong(consumerTransaction.timestamp)
  }

  def fromByteArray(bytes: Array[Byte]): ConsumerTransactionValue = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val transactionId = buffer.getLong
    val timestamp = buffer.getLong
    ConsumerTransactionValue(transactionId, timestamp)
  }
}
