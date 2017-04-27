package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

class TransactionID(val id: Long) extends AnyVal {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    TransactionID.objectToEntry(this, databaseEntry)
    databaseEntry
  }
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
      .putLong(id)
      .array()
  }
}

object TransactionID extends TupleBinding[TransactionID]  {
  def apply(id: Long): TransactionID = new TransactionID(id)

  override def entryToObject(input: TupleInput): TransactionID = TransactionID(input.readLong())
  override def objectToEntry(transactionID: TransactionID, output: TupleOutput): Unit = output.writeLong(transactionID.id)

  def fromByteArray(bytes: Array[Byte]): TransactionID = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val id = buffer.getLong
    TransactionID(id)
  }
}
