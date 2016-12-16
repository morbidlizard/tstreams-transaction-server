package transactionService.server.transactionMetaService

import com.sleepycat.je.{Database, Put, Transaction, WriteOptions}
import transactionService.rpc.TransactionStates

case class ProducerTransactionKey(key: Key, producerTransaction: ProducerTransaction) extends transactionService.rpc.ProducerTransaction {
  override def stream: String = key.stream.toString
  override def partition: Int = Integer2int(key.partition)
  override def transactionID: Long = Long2long(key.transactionID)
  override def state: TransactionStates = producerTransaction.state
  override def quantity: Int = Integer2int(producerTransaction.quantity)
  override def keepAliveTTL: Long = Long2long(producerTransaction.keepAliveTTL)
  override def toString: String = s"Producer transaction: ${key.toString}"
  def put(database: Database, txn: Transaction, putType: Put, options: WriteOptions = new WriteOptions()) =
    database.put(txn, key.toDatabaseEntry, producerTransaction.toDatabaseEntry, putType, options)
  def delete(database: Database, txn: Transaction) =  database.delete(txn, key.toDatabaseEntry)

}
object ProducerTransactionKey {
  def apply(txn: transactionService.rpc.ProducerTransaction, streamNameToLong: java.lang.Long): ProducerTransactionKey = {
    val key = Key(streamNameToLong, int2Integer(txn.partition), long2Long(txn.transactionID))
    val producerTransaction = ProducerTransaction(txn.state, int2Integer(txn.quantity), long2Long(txn.keepAliveTTL))
    ProducerTransactionKey(key, producerTransaction)
  }
}

