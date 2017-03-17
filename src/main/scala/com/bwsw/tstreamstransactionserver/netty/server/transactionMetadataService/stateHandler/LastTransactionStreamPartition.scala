package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.google.common.cache.Cache
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

  private final def fillLastTransactionStreamPartitionTable: Cache[KeyStreamPartition, TransactionID] = {
    val hoursToLive = 1
    val cache = com.google.common.cache.CacheBuilder.newBuilder()
      .expireAfterAccess(hoursToLive, TimeUnit.HOURS)
      .build[KeyStreamPartition, TransactionID]()

    val keyFound = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val cursor = lastTransactionDatabase.openCursor(new DiskOrderedCursorConfig())
    while (cursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      cache.put(KeyStreamPartition.entryToObject(keyFound), TransactionID.entryToObject(dataFound))
    }
    cursor.close()

    cache
  }

  private final val lastTransactionStreamPartitionRamTable = fillLastTransactionStreamPartitionTable

  final def getLastTransactionID(stream: Long, partition: Int): Option[TransactionID] = {
    val key = KeyStreamPartition(stream, partition)
    val lastTransactionOpt = Option(lastTransactionStreamPartitionRamTable.getIfPresent(key))
    if (lastTransactionOpt.isDefined) lastTransactionOpt
    else {
      val dataFound = new DatabaseEntry()
      if (lastTransactionDatabase.get(null, key.toDatabaseEntry, dataFound, null) == OperationStatus.SUCCESS)
        Some(TransactionID.entryToObject(dataFound))
      else
        None
    }
  }

  final def putLastTransaction(key: KeyStreamPartition, transactionId: Long, checkpointedTransactionID: Option[Long] = None, transaction: Transaction) = {
    val updatedTransactionID = new TransactionID(transactionId, checkpointedTransactionID)
    lastTransactionDatabase.put(transaction, key.toDatabaseEntry, updatedTransactionID.toDatabaseEntry)
  }

  final def updateLastTransactionStreamPartitionRamTable(key: KeyStreamPartition, transactionId: Long, checkpointedTransactionID: Option[Long] = None) = {
    lastTransactionStreamPartitionRamTable.put(key, new TransactionID(transactionId, checkpointedTransactionID))
  }

  final def getLastTransactionStreamPartitionRamTable(key: KeyStreamPartition) = lastTransactionStreamPartitionRamTable.getIfPresent(key)

  final def isThatTransactionOutOfOrder(key: KeyStreamPartition, transactionThatId: Long) = {
    val lastTransactionOpt = Option(lastTransactionStreamPartitionRamTable.getIfPresent(key))
    lastTransactionOpt match {
      case Some(transactionId) => if (transactionId.transaction <= transactionThatId) false else true
      case None => false
    }
  }
}
