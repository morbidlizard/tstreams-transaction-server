package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.sleepycat.je.{Database, Put, Transaction, WriteOptions}

case class ConsumerTransactionRecord(key: ConsumerTransactionKey, consumerTransaction: ConsumerTransactionValue)
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

object ConsumerTransactionRecord {
  def apply(txn: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction, streamNameToLong: java.lang.Long, timestamp: Long): ConsumerTransactionRecord = {
    val key = ConsumerTransactionKey(txn.name, streamNameToLong, txn.partition)
    val producerTransaction = ConsumerTransactionValue(txn.transactionID, timestamp)
    ConsumerTransactionRecord(key, producerTransaction)
  }
}
