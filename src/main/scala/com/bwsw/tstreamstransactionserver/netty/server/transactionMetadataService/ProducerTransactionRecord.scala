package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

case class ProducerTransactionRecord(key: ProducerTransactionKey, producerTransaction: ProducerTransactionValue) {
  def stream: Long = key.stream
  def partition: Int = key.partition
  def transactionID: Long = key.transactionID
  def state: TransactionStates = producerTransaction.state
  def quantity: Int = producerTransaction.quantity
  def ttl: Long = producerTransaction.ttl
  def timestamp: Long = producerTransaction.timestamp
}
object ProducerTransactionRecord {
  def apply(txn: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction, streamID: Long, timestamp: Long): ProducerTransactionRecord = {
    val key = ProducerTransactionKey(streamID, txn.partition, txn.transactionID)
    val producerTransaction = ProducerTransactionValue(txn.state, txn.quantity, txn.ttl, timestamp)
    ProducerTransactionRecord(key, producerTransaction)
  }
}

