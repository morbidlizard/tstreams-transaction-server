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
    val storeName = "LastOpenedTransactionStorage"
    environment.openDatabase(null, storeName, dbConfig)
  }

  private final val lastCheckpointedTransactionDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    val storeName = "LastCheckpointedTransactionStorage"
    environment.openDatabase(null, storeName, dbConfig)
  }


  private final def fillLastTransactionStreamPartitionTable: Cache[KeyStreamPartition, LastOpenedAndCheckpointedTransaction] = {
    val hoursToLive = 1
    val cache = com.google.common.cache.CacheBuilder.newBuilder()
      .expireAfterAccess(hoursToLive, TimeUnit.HOURS)
      .build[KeyStreamPartition, LastOpenedAndCheckpointedTransaction]()

    val keyFound = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val lastTransactionDatabaseCursor = lastTransactionDatabase.openCursor(new DiskOrderedCursorConfig())
    while (lastTransactionDatabaseCursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      cache.put(KeyStreamPartition.entryToObject(keyFound), LastOpenedAndCheckpointedTransaction(TransactionID.entryToObject(dataFound), None))
    }
    lastTransactionDatabaseCursor.close()

    val lastCheckpointedTransactionDatabaseCursor = lastCheckpointedTransactionDatabase.openCursor(new DiskOrderedCursorConfig())
    while (lastCheckpointedTransactionDatabaseCursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      val lastOpenedAndCheckpointedOpt = Some(cache.getIfPresent(KeyStreamPartition.entryToObject(keyFound)))
      lastOpenedAndCheckpointedOpt foreach { x =>
        cache.put(
          KeyStreamPartition.entryToObject(keyFound),
          LastOpenedAndCheckpointedTransaction(x.opened, Some(TransactionID.entryToObject(dataFound)))
        )
      }
    }
    lastCheckpointedTransactionDatabaseCursor.close()
    cache
  }

  private final val lastTransactionStreamPartitionRamTable: Cache[KeyStreamPartition, LastOpenedAndCheckpointedTransaction] = fillLastTransactionStreamPartitionTable

  final def getLastTransactionIDAndCheckpointedID(stream: Long, partition: Int): Option[LastOpenedAndCheckpointedTransaction] = {
    val key = KeyStreamPartition(stream, partition)
    val lastTransactionOpt = Option(lastTransactionStreamPartitionRamTable.getIfPresent(key))
    if (lastTransactionOpt.isDefined) lastTransactionOpt
    else {
      val dataFound = new DatabaseEntry()
      val binaryKey = key.toDatabaseEntry
      val lastOpenedTransaction =
        if (lastTransactionDatabase.get(null, binaryKey, dataFound, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
          Some(TransactionID.entryToObject(dataFound))
        else None
      lastOpenedTransaction match {
        case Some(openedTransaction) =>
          val lastCheckpointed =
            if (lastCheckpointedTransactionDatabase.get(null, binaryKey, dataFound, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
              Some(TransactionID.entryToObject(dataFound))
            else None

          Some(LastOpenedAndCheckpointedTransaction(openedTransaction, lastCheckpointed))
        case None => None
      }
    }
  }

  final def putLastTransaction(key: KeyStreamPartition, transactionId: Long, isOpenedTransaction: Boolean, transaction: com.sleepycat.je.Transaction) = {
    val updatedTransactionID = new TransactionID(transactionId)
    if (isOpenedTransaction)
      lastTransactionDatabase.put(transaction, key.toDatabaseEntry, updatedTransactionID.toDatabaseEntry)
    else
      lastCheckpointedTransactionDatabase.put(transaction, key.toDatabaseEntry, updatedTransactionID.toDatabaseEntry)
  }

  final def updateLastTransactionStreamPartitionRamTable(key: KeyStreamPartition, transaction: Long, isOpenedTransaction: Boolean) = {
    val lastOpenedAndCheckpointedTransaction = Option(lastTransactionStreamPartitionRamTable.getIfPresent(key))
    lastOpenedAndCheckpointedTransaction match {
      case Some(x) =>
        if (isOpenedTransaction)
          lastTransactionStreamPartitionRamTable.put(key, LastOpenedAndCheckpointedTransaction(TransactionID(transaction), x.checkpointed))
        else
          lastTransactionStreamPartitionRamTable.put(key, LastOpenedAndCheckpointedTransaction(x.opened, Some(TransactionID(transaction))))
      case None if isOpenedTransaction => lastTransactionStreamPartitionRamTable.put(key, LastOpenedAndCheckpointedTransaction(TransactionID(transaction), None))
      case _ => //do nothing
    }
  }

  final def updateLastTransactionStreamPartitionRamTable(key: KeyStreamPartition, openedTransaction: Long, checkpointedTransaction: Long) = {
    lastTransactionStreamPartitionRamTable.put(key, LastOpenedAndCheckpointedTransaction(TransactionID(openedTransaction), Some(TransactionID(checkpointedTransaction))))
  }

  final def isThatTransactionOutOfOrder(key: KeyStreamPartition, transactionThatId: Long) = {
    val lastTransactionOpt = Option(lastTransactionStreamPartitionRamTable.getIfPresent(key))
    lastTransactionOpt match {
      case Some(transactionId) => if (transactionId.opened.id <= transactionThatId) false else true
      case None => false
    }
  }

  def closeLastTransactionStreamPartitionDatabase() = {
    scala.util.Try(lastTransactionDatabase.close())
  }
}
