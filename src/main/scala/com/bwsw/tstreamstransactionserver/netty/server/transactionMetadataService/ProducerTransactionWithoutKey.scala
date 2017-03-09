package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionWithoutKey.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry
import transactionService.rpc.TransactionStates

case class ProducerTransactionWithoutKey(state: TransactionStates, quantity: java.lang.Integer, ttl: java.lang.Long, timestamp: java.lang.Long) {
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
    val ttl = input.readLong()
    val timestamp = input.readLong()
    ProducerTransactionWithoutKey(state, int2Integer(quantity), long2Long(ttl), long2Long(timestamp))
  }
  override def objectToEntry(producerTransaction: ProducerTransactionWithoutKey, output: TupleOutput): Unit = {
    output.writeInt(producerTransaction.state.value)
    output.writeInt(Integer2int(producerTransaction.quantity))
    output.writeLong(Long2long(producerTransaction.ttl))
    output.writeLong(Long2long(producerTransaction.timestamp))
  }
}
