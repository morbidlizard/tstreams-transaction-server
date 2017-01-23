package com.bwsw.netty.server.сonsumerService

import com.sleepycat.je.{Database, Put, Transaction, WriteOptions}

case class ConsumerTransactionKey(key: Key, consumerTransaction: ConsumerTransaction)
  extends transactionService.rpc.ConsumerTransaction
{
  override def transactionID: Long = consumerTransaction.transactionId
  override def name: String = key.name
  override def stream: String = key.stream.toString
  override def partition: Int = key.partition
  override def toString: String = s"Consumer transaction: ${key.toString}"

  def put(database: Database, txn: Transaction, putType: Put, options: WriteOptions) =
    database.put(txn, key.toDatabaseEntry, consumerTransaction.toDatabaseEntry, putType, options)
}

object ConsumerTransactionKey {
  def apply(txn: transactionService.rpc.ConsumerTransaction, streamNameToLong: java.lang.Long): ConsumerTransactionKey = {
    val key = Key(txn.name, streamNameToLong, txn.partition)
    val producerTransaction = ConsumerTransaction(txn.transactionID)
    ConsumerTransactionKey(key, producerTransaction)
  }
}
