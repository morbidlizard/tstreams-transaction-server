package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.sleepycat.bind.tuple.LongBinding
import com.sleepycat.je.DatabaseEntry

class CommitLogKey(val id: Long) extends AnyVal{
  def keyToDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    LongBinding.longToEntry(id, databaseEntry)
    databaseEntry
  }
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
      .putLong(id)
      .array()
  }
}

object CommitLogKey {
  def apply(id: Long): CommitLogKey = new CommitLogKey(id)
  def keyToObject(databaseEntry: DatabaseEntry) =
    new CommitLogKey(LongBinding.entryToLong(databaseEntry))

  def fromByteArray(bytes: Array[Byte]): CommitLogKey = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val id = buffer.getLong()
    CommitLogKey(id)
  }
}
