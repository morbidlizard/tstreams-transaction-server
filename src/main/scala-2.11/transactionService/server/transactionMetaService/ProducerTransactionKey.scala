package transactionService.server.transactionMetaService

import com.sleepycat.je.{Database, Put, Transaction, WriteOptions}
import transactionService.rpc.TransactionStates

case class ProducerTransactionKey(key: Key, producerTransaction: ProducerTransaction) extends transactionService.rpc.ProducerTransaction {
  override def stream: String = key.stream.toString
  override def partition: Int = key.partition
  override def transactionID: Long = key.transactionID
  override def state: TransactionStates = producerTransaction.state
  override def quantity: Int = producerTransaction.quantity
  override def keepAliveTTL: Long = producerTransaction.keepAliveTTL
  override def toString: String = s"Producer transaction: ${key.toString}"
  def put(database: Database, txn: Transaction, putType: Put, options: WriteOptions = new WriteOptions()) =
    database.put(txn, key.toDatabaseEntry, producerTransaction.toDatabaseEntry, putType, options)
  def delete(database: Database, txn: Transaction) = database.delete(txn, key.toDatabaseEntry)

}
object ProducerTransactionKey {
  def apply(txn: transactionService.rpc.ProducerTransaction, streamNameToLong: java.lang.Long): ProducerTransactionKey = {
    val key = Key(streamNameToLong, txn.partition, txn.transactionID)
    val producerTransaction = ProducerTransaction(txn.state,txn.quantity,txn.keepAliveTTL)
    ProducerTransactionKey(key, producerTransaction)
  }
}

