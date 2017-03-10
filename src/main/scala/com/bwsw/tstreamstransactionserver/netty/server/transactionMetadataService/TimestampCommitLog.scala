package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.sleepycat.bind.tuple.{LongBinding, StringBinding}
import com.sleepycat.je.DatabaseEntry

case class TimestampCommitLog(timestamp: Long, pathToFile: String) {
  def keyToDatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    LongBinding.longToEntry(timestamp, databaseEntry)
    databaseEntry
  }
  def dataToDatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    StringBinding.stringToEntry(pathToFile, databaseEntry)
    databaseEntry
  }
}

object TimestampCommitLog {
  def keyToObject(databaseEntry: DatabaseEntry) =
    LongBinding.entryToLong(databaseEntry)

  def pathToObject(databaseEntry: DatabaseEntry) =
    StringBinding.entryToString(databaseEntry)
}
