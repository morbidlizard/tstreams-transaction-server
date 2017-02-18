package com.bwsw.tstreamstransactionserver.netty.server.transactionMetaService

import com.sleepycat.bind.tuple.TupleBinding
import com.sleepycat.je.DatabaseEntry

case class TimestampCommitLog(timestamp: Long, pathToFile: String) {
  def keyToDatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    TupleBinding.getPrimitiveBinding(classOf[java.lang.Long]).objectToEntry(timestamp, databaseEntry)
    databaseEntry
  }
  def dataToDatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    TupleBinding.getPrimitiveBinding(classOf[java.lang.String]).objectToEntry(pathToFile, databaseEntry)
    databaseEntry
  }
}

object TimestampCommitLog {
  def keyToObject(databaseEntry: DatabaseEntry) =
    TupleBinding.getPrimitiveBinding(classOf[java.lang.Long]).entryToObject(databaseEntry)

  def pathToObject(databaseEntry: DatabaseEntry) =
    TupleBinding.getPrimitiveBinding(classOf[java.lang.String]).entryToObject(databaseEntry)
}
