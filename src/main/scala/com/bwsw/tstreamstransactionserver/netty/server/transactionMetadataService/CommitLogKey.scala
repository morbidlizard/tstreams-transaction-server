package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.sleepycat.bind.tuple.{LongBinding, StringBinding}
import com.sleepycat.je.DatabaseEntry

class CommitLogKey(val id: Long) extends AnyVal {
  def keyToDatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    LongBinding.longToEntry(id, databaseEntry)
    databaseEntry
  }
}

object CommitLogKey {
  def apply(id: Long): CommitLogKey = new CommitLogKey(id)
  def keyToObject(databaseEntry: DatabaseEntry) =
    new CommitLogKey(LongBinding.entryToLong(databaseEntry))
}
