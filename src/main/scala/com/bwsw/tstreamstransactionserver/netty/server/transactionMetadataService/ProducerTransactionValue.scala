package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionValue.objectToEntry
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class ProducerTransactionValue(state: TransactionStates, quantity: Int, ttl: Long, timestamp: Long) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object ProducerTransactionValue extends TupleBinding[ProducerTransactionValue]
{
  override def entryToObject(input: TupleInput): ProducerTransactionValue = {
    val state  = TransactionStates(input.readInt())
    val quantity = input.readInt()
    val ttl = input.readLong()
    val timestamp = input.readLong()
    ProducerTransactionValue(state, quantity, ttl, timestamp)
  }
  override def objectToEntry(producerTransaction: ProducerTransactionValue, output: TupleOutput): Unit = {
    output.writeInt(producerTransaction.state.value)
    output.writeInt(producerTransaction.quantity)
    output.writeLong(producerTransaction.ttl)
    output.writeLong(producerTransaction.timestamp)
  }
}
