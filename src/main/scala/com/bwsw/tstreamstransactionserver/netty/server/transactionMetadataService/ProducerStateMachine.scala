package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage

import scala.collection.mutable

private[server] final class ProducerStateMachine(rocksDB: KeyValueDatabaseManager) {
  private val producerTransactionsWithOpenedStateDatabase =
    rocksDB.getDatabase(RocksStorage.TRANSACTION_OPEN_STORE)

  private val transactionsRamTable =
    fillOpenedTransactionsRAMTable

  private def fillOpenedTransactionsRAMTable: mutable.Map[ProducerTransactionKey, ProducerTransactionValue] = {
//    if (logger.isDebugEnabled)
//      logger.debug("Filling cache with Opened Transactions table.")

    val cache = mutable.Map.empty[ProducerTransactionKey, ProducerTransactionValue]

//    val iterator = producerTransactionsWithOpenedStateDatabase.iterator
//    iterator.seekToFirst()
//    while (iterator.isValid) {
//      val key = ProducerTransactionKey
//        .fromByteArray(iterator.key())
//
//      val value = ProducerTransactionValue
//        .fromByteArray(iterator.value())
//
//      cache.put(key, value)
//      iterator.next()
//    }
//    iterator.close()

    cache
  }

  def getOpenedTransaction(key: ProducerTransactionKey): Option[ProducerTransactionValue] = {
    transactionsRamTable.get(key)
      .orElse {
        val keyFound = key.toByteArray
        Option(
          producerTransactionsWithOpenedStateDatabase.get(keyFound)
        ).map { data =>
          val producerTransactionValue =
            ProducerTransactionValue.fromByteArray(data)

          transactionsRamTable.put(
            key,
            producerTransactionValue
          )

          producerTransactionValue
        }
      }
  }

  def updateOpenedTransaction(key: ProducerTransactionKey,
                              value: ProducerTransactionValue): Unit = {
    transactionsRamTable.put(key, value)
  }

  def removeOpenedTransaction(key: ProducerTransactionKey): Unit = {
    transactionsRamTable -= key
  }

  def clear(): Unit ={
    transactionsRamTable.clear()
  }
}
