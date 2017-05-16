package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction

case class ConsumerTransactionRecord(key: ConsumerTransactionKey, consumerTransaction: ConsumerTransactionValue) extends ConsumerTransaction
{
  override def transactionID: Long = consumerTransaction.transactionId
  override def name: String = key.name
  override def stream: Int = key.streamID
  override def partition: Int = key.partition
  def timestamp: Long = Long2long(consumerTransaction.timestamp)
}

object ConsumerTransactionRecord {
  def apply(txn: ConsumerTransaction, timestamp: Long): ConsumerTransactionRecord = {
    val key = ConsumerTransactionKey(txn.name, txn.stream, txn.partition)
    val producerTransaction = ConsumerTransactionValue(txn.transactionID, timestamp)
    ConsumerTransactionRecord(key, producerTransaction)
  }
}
