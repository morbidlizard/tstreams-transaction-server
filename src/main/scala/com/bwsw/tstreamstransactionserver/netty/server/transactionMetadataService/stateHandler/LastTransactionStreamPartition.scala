package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.sleepycat.je._

trait LastTransactionStreamPartition {
  val environment: Environment
  val storageOpts: StorageOptions

  private final val lastTransactionDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    val storeName = "LastTransactionStorage"
    environment.openDatabase(null, storeName, dbConfig)
  }

  private final def fillLasTTransactionStreamPartitionTable: ConcurrentHashMap[Key, TransactionID] = {
    val cache = new java.util.concurrent.ConcurrentHashMap[Key, TransactionID]()

    val keyFound = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val cursor = lastTransactionDatabase.openCursor(new DiskOrderedCursorConfig())
    while (cursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      cache.put(Key.entryToObject(keyFound), TransactionID.entryToObject(dataFound))
    }
    cursor.close()

    cache
  }

  private final val lasTTransactionStreamPartitionRamTable = fillLasTTransactionStreamPartitionTable

  final def isThatTransactionOutOfOrder(key: Key, transactionThatId: Long, transaction: Transaction) = {
    def updateRamTableAndDatabase() = {
      val updatedTransactionID = new TransactionID(transactionThatId)
      lastTransactionDatabase.put(transaction, key.toDatabaseEntry, updatedTransactionID.toDatabaseEntry)
      lasTTransactionStreamPartitionRamTable.put(key, updatedTransactionID)
    }

    if (lasTTransactionStreamPartitionRamTable.contains(key))
      if (lasTTransactionStreamPartitionRamTable.get(key).transaction <= transactionThatId) {
        updateRamTableAndDatabase()
        false
      } else {
        true
      }
    else {
      updateRamTableAndDatabase()
      false
    }
  }
}
