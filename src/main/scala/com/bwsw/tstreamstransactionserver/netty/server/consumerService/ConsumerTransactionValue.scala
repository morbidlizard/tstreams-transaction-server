package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionValue.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry


case class ConsumerTransactionValue(transactionId: java.lang.Long, timestamp: java.lang.Long) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object ConsumerTransactionValue extends TupleBinding[ConsumerTransactionValue] {
  override def entryToObject(input: TupleInput): ConsumerTransactionValue = ConsumerTransactionValue(input.readLong(), input.readLong())
  override def objectToEntry(consumerTransaction: ConsumerTransactionValue, output: TupleOutput): Unit = {
    output.writeLong(consumerTransaction.transactionId)
    output.writeLong(consumerTransaction.timestamp)
  }
}
