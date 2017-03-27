package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class TransactionID(id: Long) extends AnyVal {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    TransactionID.objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object TransactionID extends TupleBinding[TransactionID]  {
  override def entryToObject(input: TupleInput): TransactionID = TransactionID(input.readLong())
  override def objectToEntry(transactionID: TransactionID, output: TupleOutput): Unit = output.writeLong(transactionID.id)
}
