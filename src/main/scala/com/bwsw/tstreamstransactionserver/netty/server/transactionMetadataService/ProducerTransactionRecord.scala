package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction

case class ProducerTransactionRecord(key: ProducerTransactionKey,
                                     producerTransaction: ProducerTransactionValue
                                    )
  extends ProducerTransaction
    with Ordered[ProducerTransactionRecord] {
  override def stream: Int = key.stream

  override def partition: Int = key.partition

  override def transactionID: Long = key.transactionID

  override def state: TransactionStates = producerTransaction.state

  override def quantity: Int = producerTransaction.quantity

  override def ttl: Long = producerTransaction.ttl

  def timestamp: Long = producerTransaction.timestamp

  override def compare(that: ProducerTransactionRecord): Int = {
    if (this.stream < that.stream) -1
    else if (this.stream > that.stream) 1
    else if (this.partition < that.partition) -1
    else if (this.partition > that.partition) 1
    else if (this.transactionID < that.transactionID) -1
    else if (this.transactionID > that.transactionID) 1
    else if (this.timestamp < that.timestamp) -1
    else if (this.timestamp > that.timestamp) 1
    else if (this.state.value < that.state.value) -1
    else if (this.state.value > that.state.value) 1
    else 0
  }
}

object ProducerTransactionRecord {
  def apply(txn: ProducerTransaction,
            timestamp: Long): ProducerTransactionRecord = {
    val key = ProducerTransactionKey(txn.stream, txn.partition, txn.transactionID)
    val producerTransaction = ProducerTransactionValue(txn.state, txn.quantity, txn.ttl, timestamp)
    ProducerTransactionRecord(key, producerTransaction)
  }
}

