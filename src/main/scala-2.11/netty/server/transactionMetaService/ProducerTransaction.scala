package netty.server.transactionMetaService

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry
import transactionService.rpc.TransactionStates
import ProducerTransaction.objectToEntry

case class ProducerTransaction(state: TransactionStates, quantity: java.lang.Integer, keepAliveTTL: java.lang.Long) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object ProducerTransaction extends TupleBinding[ProducerTransaction]
{
  override def entryToObject(input: TupleInput): ProducerTransaction = {
    val state  = TransactionStates(input.readInt())
    val quantity = input.readInt()
    val timestamp = input.readLong()
    ProducerTransaction(state, int2Integer(quantity), long2Long(timestamp))
  }
  override def objectToEntry(producerTransaction: ProducerTransaction, output: TupleOutput): Unit = {
    output.writeInt(producerTransaction.state.value)
    output.writeInt(Integer2int(producerTransaction.quantity))
    output.writeLong(Long2long(producerTransaction.keepAliveTTL))
  }
}
