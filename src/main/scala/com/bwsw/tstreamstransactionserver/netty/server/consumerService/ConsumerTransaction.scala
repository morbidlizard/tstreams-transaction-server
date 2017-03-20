package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry
import ConsumerTransaction.objectToEntry


case class ConsumerTransaction(transactionId: Long) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object ConsumerTransaction extends TupleBinding[ConsumerTransaction] {
  override def entryToObject(input: TupleInput): ConsumerTransaction = ConsumerTransaction(input.readLong())
  override def objectToEntry(consumerTransaction: ConsumerTransaction, output: TupleOutput): Unit = {
    output.writeLong(consumerTransaction.transactionId)
  }
}
