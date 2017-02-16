package com.bwsw.tstreamstransactionserver.netty.server.transactionMetaService

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry
import transactionService.rpc.TransactionStates
import ProducerTransactionWithoutKey.objectToEntry

case class ProducerTransactionWithoutKey(state: TransactionStates, quantity: java.lang.Integer, keepAliveTTL: java.lang.Long, timestamp: java.lang.Long) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object ProducerTransactionWithoutKey extends TupleBinding[ProducerTransactionWithoutKey]
{
  override def entryToObject(input: TupleInput): ProducerTransactionWithoutKey = {
    val state  = TransactionStates(input.readInt())
    val quantity = input.readInt()
    val keepAliveTTL = input.readLong()
    val timestamp = input.readLong()
    ProducerTransactionWithoutKey(state, int2Integer(quantity), long2Long(keepAliveTTL), long2Long(timestamp))
  }
  override def objectToEntry(producerTransaction: ProducerTransactionWithoutKey, output: TupleOutput): Unit = {
    output.writeInt(producerTransaction.state.value)
    output.writeInt(Integer2int(producerTransaction.quantity))
    output.writeLong(Long2long(producerTransaction.keepAliveTTL))
    output.writeLong(Long2long(producerTransaction.timestamp))
  }
}
