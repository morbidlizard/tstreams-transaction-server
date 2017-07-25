package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.TransactionStateHandler
import org.slf4j.{Logger, LoggerFactory}

class Cleaner(rocksDB: KeyValueDatabaseManager) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private val producerTransactionsWithOpenedStateDatabase =
    rocksDB.getDatabase(RocksStorage.TRANSACTION_OPEN_STORE)

  protected def onProducerTransactionStateChangeDo: ProducerTransactionRecord => Unit =
    _ => {}

  def transactionsToDeleteTask(timestampToDeleteTransactions: Long) {
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
        val producerTransactionValueTimestampUpdated =
          producerTransactionValue.copy(timestamp = timestampToDeleteTransactions)
        val canceledTransactionRecordDueExpiration = TransactionStateHandler
          .transitProducerTransactionToInvalidState(
            ProducerTransactionRecord(
              producerTransactionKey,
              producerTransactionValueTimestampUpdated
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
    transactionsToDeleteTask(timestampToDeleteTransactions)
  }
}
