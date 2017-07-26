package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.Invalid
import org.slf4j.{Logger, LoggerFactory}

class ProducerTransactionsCleaner(rocksDB: KeyValueDatabaseManager) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private val producerTransactionsWithOpenedStateDatabase =
    rocksDB.getDatabase(RocksStorage.TRANSACTION_OPEN_STORE)

  protected def onProducerTransactionStateChangeDo: ProducerTransactionRecord => Unit =
    _ => {}

  private def transitProducerTransactionToInvalidState(producerTransactionRecord: ProducerTransactionRecord) = {
    val txn = producerTransactionRecord
    ProducerTransactionRecord(
      ProducerTransactionKey(txn.stream, txn.partition, txn.transactionID),
      ProducerTransactionValue(Invalid, 0, 0L, txn.timestamp)
    )
  }

  def cleanExpiredProducerTransactions(timestampToDeleteTransactions: Long) {
    def doesProducerTransactionExpired(producerTransactionWithoutKey: ProducerTransactionValue): Boolean = {
      scala.math.abs(
        producerTransactionWithoutKey.timestamp +
          producerTransactionWithoutKey.ttl
      ) <= timestampToDeleteTransactions
    }


    if (logger.isDebugEnabled)
      logger.debug(s"Cleaner[time: $timestampToDeleteTransactions] of expired transactions is running.")
    val batch = rocksDB.newBatch

    val iterator = producerTransactionsWithOpenedStateDatabase.iterator
    iterator.seekToFirst()

    while (iterator.isValid) {
      val producerTransactionValue =
        ProducerTransactionValue.fromByteArray(iterator.value())

      if (doesProducerTransactionExpired(producerTransactionValue)) {
        if (logger.isDebugEnabled)
          logger.debug(s"Cleaning $producerTransactionValue as it's expired.")

        val key =
          iterator.key()

        val producerTransactionKey =
          ProducerTransactionKey.fromByteArray(key)

        val canceledTransactionRecordDueExpiration =
          transitProducerTransactionToInvalidState(
            ProducerTransactionRecord(
              producerTransactionKey,
              producerTransactionValue
            )
          )

        onProducerTransactionStateChangeDo(
          canceledTransactionRecordDueExpiration
        )

        batch.put(
          RocksStorage.TRANSACTION_ALL_STORE,
          key,
          canceledTransactionRecordDueExpiration
            .producerTransaction.toByteArray
        )

        batch.remove(
          RocksStorage.TRANSACTION_OPEN_STORE,
          key
        )
      }
      iterator.next()
    }
    iterator.close()
    batch.write()
  }

  final def createAndExecuteTransactionsToDeleteTask(timestampToDeleteTransactions: Long): Unit = {
    cleanExpiredProducerTransactions(timestampToDeleteTransactions)
  }
}
