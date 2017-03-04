package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.sleepycat.je.{Database, Put, Transaction, WriteOptions}
import transactionService.rpc.TransactionStates

case class ProducerTransactionKey(key: Key, producerTransaction: ProducerTransactionWithoutKey) {
  def stream: Long = Long2long(key.stream)
  def partition: Int = Integer2int(key.partition)
  def transactionID: Long = Long2long(key.transactionID)
  def state: TransactionStates = producerTransaction.state
  def quantity: Int = Integer2int(producerTransaction.quantity)
  def keepAliveTTL: Long = Long2long(producerTransaction.ttl)
  def timestamp: Long = Long2long(producerTransaction.timestamp)
  def put(database: Database, txn: Transaction, putType: Put, options: WriteOptions = new WriteOptions()) =
    database.put(txn, key.toDatabaseEntry, producerTransaction.toDatabaseEntry, putType, options)
  def delete(database: Database, txn: Transaction) =  database.delete(txn, key.toDatabaseEntry)

  override  def toString: String = s"Producer transaction: ${key.toString}, state: $state"
}
object ProducerTransactionKey {
  def apply(txn: transactionService.rpc.ProducerTransaction, streamNameToLong: java.lang.Long, timestamp: Long): ProducerTransactionKey = {
    val key = Key(streamNameToLong, int2Integer(txn.partition), long2Long(txn.transactionID))
    val producerTransaction = ProducerTransactionWithoutKey(txn.state, int2Integer(txn.quantity), long2Long(txn.ttl), long2Long(timestamp))
    ProducerTransactionKey(key, producerTransaction)
  }
}

