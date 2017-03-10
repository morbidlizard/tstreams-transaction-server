package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.sleepycat.je.{Database, Put, Transaction, WriteOptions}

case class ConsumerTransactionKey(key: Key, consumerTransaction: ConsumerTransactionWithoutKey)
{
  def transactionID: Long = consumerTransaction.transactionId
  def name: String = key.name
  def stream: Long = Long2long(key.stream)
  def partition: Int = key.partition
  def timestamp: Long = Long2long(consumerTransaction.timestamp)
  override def toString: String = s"Consumer transaction: ${key.toString}"

  def put(database: Database, txn: Transaction, putType: Put, options: WriteOptions) =
    database.put(txn, key.toDatabaseEntry, consumerTransaction.toDatabaseEntry, putType, options)
}

object ConsumerTransactionKey {
  def apply(txn: transactionService.rpc.ConsumerTransaction, streamNameToLong: java.lang.Long, timestamp: Long): ConsumerTransactionKey = {
    val key = Key(txn.name, streamNameToLong, txn.partition)
    val producerTransaction = ConsumerTransactionWithoutKey(txn.transactionID, timestamp)
    ConsumerTransactionKey(key, producerTransaction)
  }
}
