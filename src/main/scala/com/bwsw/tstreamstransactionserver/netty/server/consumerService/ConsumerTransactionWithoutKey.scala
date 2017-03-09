package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionWithoutKey.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry


case class ConsumerTransactionWithoutKey(transactionId: java.lang.Long, timestamp: java.lang.Long) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object ConsumerTransactionWithoutKey extends TupleBinding[ConsumerTransactionWithoutKey] {
  override def entryToObject(input: TupleInput): ConsumerTransactionWithoutKey = ConsumerTransactionWithoutKey(input.readLong(), input.readLong())
  override def objectToEntry(consumerTransaction: ConsumerTransactionWithoutKey, output: TupleOutput): Unit = {
    output.writeLong(consumerTransaction.transactionId)
    output.writeLong(consumerTransaction.timestamp)
  }
}
