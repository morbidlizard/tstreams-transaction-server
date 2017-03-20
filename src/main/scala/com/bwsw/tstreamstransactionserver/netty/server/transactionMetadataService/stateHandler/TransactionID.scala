package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

class TransactionID(val transaction: Long, val checkpointedTransactionOpt: Option[Long]) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    TransactionID.objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object TransactionID extends TupleBinding[TransactionID] {
  override def entryToObject(input: TupleInput): TransactionID = {
    //two longs must give 8+8 bytes
    if (input.getBufferLength < 16) new TransactionID(input.readLong(), None)
    else new TransactionID(input.readLong(), Some(input.readLong()))
  }
  override def objectToEntry(transactionID: TransactionID, output: TupleOutput): Unit = {
    output.writeLong(transactionID.transaction)
    if (transactionID.checkpointedTransactionOpt.isDefined)
      output.writeLong(transactionID.checkpointedTransactionOpt.get)
  }
}
