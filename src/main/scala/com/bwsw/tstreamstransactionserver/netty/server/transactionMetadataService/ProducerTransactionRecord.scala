package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.sleepycat.je.{Database, Put, Transaction, WriteOptions}

case class ProducerTransactionRecord(key: ProducerTransactionKey, producerTransaction: ProducerTransactionValue) {
  def stream: Long = key.stream
  def partition: Int = key.partition
  def transactionID: Long = key.transactionID
  def state: TransactionStates = producerTransaction.state
  def quantity: Int = producerTransaction.quantity
  def ttl: Long = producerTransaction.ttl
  def timestamp: Long = producerTransaction.timestamp
  def put(database: Database, txn: Transaction, putType: Put, options: WriteOptions = new WriteOptions()) =
    database.put(txn, key.toDatabaseEntry, producerTransaction.toDatabaseEntry, putType, options)
  def delete(database: Database, txn: Transaction) =  database.delete(txn, key.toDatabaseEntry)

  override  def toString: String = s"Producer transaction: ${key.toString}, state: $state"
}
object ProducerTransactionRecord {
  def apply(txn: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction, streamNameToLong: Long, timestamp: Long): ProducerTransactionRecord = {
    val key = ProducerTransactionKey(streamNameToLong, txn.partition, txn.transactionID)
    val producerTransaction = ProducerTransactionValue(txn.state, txn.quantity, txn.ttl, timestamp)
    ProducerTransactionRecord(key, producerTransaction)
  }
}

