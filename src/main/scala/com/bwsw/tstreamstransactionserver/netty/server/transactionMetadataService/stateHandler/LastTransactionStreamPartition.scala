package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import java.util.concurrent.{Callable, TimeUnit}

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.google.common.cache.Cache
import com.sleepycat.je._

trait LastTransactionStreamPartition {
  val environment: Environment
  val executionContext: ServerExecutionContext
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
      val lastOpenedAndCheckpointedOpt = Option(cache.getIfPresent(KeyStreamPartition.entryToObject(keyFound)))
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

  final def getLastTransactionIDAndCheckpointedID(stream: Long, partition: Int, transaction: com.sleepycat.je.Transaction): Option[LastOpenedAndCheckpointedTransaction] = {
    val key = KeyStreamPartition(stream, partition)
    val lastTransactionOpt = Option(lastTransactionStreamPartitionRamTable.getIfPresent(key))
    if (lastTransactionOpt.isDefined) lastTransactionOpt
    else {
      val dataFound = new DatabaseEntry()
      val binaryKey = key.toDatabaseEntry
      val lastOpenedTransaction =
        if (lastTransactionDatabase.get(transaction, binaryKey, dataFound, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
          Some(TransactionID.entryToObject(dataFound))
        else None
      val result = lastOpenedTransaction match {
        case Some(openedTransaction) =>
          val lastCheckpointed =
            if (lastCheckpointedTransactionDatabase.get(transaction, binaryKey, dataFound, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
              Some(TransactionID.entryToObject(dataFound))
            else None

          Some(LastOpenedAndCheckpointedTransaction(openedTransaction, lastCheckpointed))
        case None => None
      }
      result
    }
  }


  private class DeleteLastOpenedAndCheckpointedTransactions(stream: Long, transaction: com.sleepycat.je.Transaction) extends Callable[Unit]{
    override def call(): Unit = {
      val from = KeyStreamPartition(stream, Int.MinValue).toDatabaseEntry
      val to   = KeyStreamPartition(stream, Int.MaxValue).toDatabaseEntry

      val lockMode = LockMode.READ_UNCOMMITTED

      val dataFound = new DatabaseEntry()

      var binaryKey = from
      val lastTransactionDatabaseCursor = lastTransactionDatabase.openCursor(transaction, new CursorConfig())
      if (lastTransactionDatabaseCursor.getSearchKeyRange(binaryKey, dataFound, lockMode) == OperationStatus.SUCCESS &&
        lastTransactionDatabase.compareKeys(binaryKey, from) >= 0 && lastTransactionDatabase.compareKeys(binaryKey, to) <= 0) {
        lastTransactionDatabaseCursor.delete()
        while (lastTransactionDatabaseCursor.getNext(binaryKey, dataFound, lockMode) == OperationStatus.SUCCESS && lastTransactionDatabase.compareKeys(binaryKey, to) <= 0) {
          lastTransactionDatabaseCursor.delete()
        }
      }
      lastTransactionDatabaseCursor.close()

      binaryKey = from
      val lastCheckpointedTransactionDatabaseCursor = lastCheckpointedTransactionDatabase.openCursor(transaction, new CursorConfig())
      if (lastCheckpointedTransactionDatabaseCursor.getSearchKeyRange(binaryKey, dataFound, lockMode) == OperationStatus.SUCCESS &&
        lastCheckpointedTransactionDatabase.compareKeys(binaryKey, from) >= 0 && lastCheckpointedTransactionDatabase.compareKeys(binaryKey, to) <= 0) {
        lastCheckpointedTransactionDatabaseCursor.delete()
        while (lastCheckpointedTransactionDatabaseCursor.getNext(binaryKey, dataFound, lockMode) == OperationStatus.SUCCESS && lastCheckpointedTransactionDatabase.compareKeys(binaryKey, to) <= 0) {
          lastCheckpointedTransactionDatabaseCursor.delete()
        }
      }
      binaryKey = null
      lastCheckpointedTransactionDatabaseCursor.close()
    }
  }

  final def deleteLastOpenedAndCheckpointedTransactions(stream: Long, transaction: com.sleepycat.je.Transaction): Unit = {
    executionContext.berkeleyWriteContext.submit(new DeleteLastOpenedAndCheckpointedTransactions(stream, transaction)).get()
  }

  private[transactionMetadataService] final def putLastTransaction(key: KeyStreamPartition, transactionId: Long, isOpenedTransaction: Boolean, transaction: com.sleepycat.je.Transaction) = {
    val updatedTransactionID = new TransactionID(transactionId)
    if (isOpenedTransaction)
      lastTransactionDatabase.put(transaction, key.toDatabaseEntry, updatedTransactionID.toDatabaseEntry)
    else
      lastCheckpointedTransactionDatabase.put(transaction, key.toDatabaseEntry, updatedTransactionID.toDatabaseEntry)
  }

  private[transactionMetadataService] def updateLastTransactionStreamPartitionRamTable(key: KeyStreamPartition, transaction: Long, isOpenedTransaction: Boolean) = {
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

  private[transactionMetadataService] final def updateLastTransactionStreamPartitionRamTable(key: KeyStreamPartition, openedTransaction: Long, checkpointedTransaction: Long): Unit = {
    lastTransactionStreamPartitionRamTable.put(key, LastOpenedAndCheckpointedTransaction(TransactionID(openedTransaction), Some(TransactionID(checkpointedTransaction))))
  }

  private[transactionMetadataService] final def isThatTransactionOutOfOrder(key: KeyStreamPartition, transactionThatId: Long) = {
    val lastTransactionOpt = Option(lastTransactionStreamPartitionRamTable.getIfPresent(key))
    lastTransactionOpt match {
      case Some(transactionId) => if (transactionId.opened.id < transactionThatId) false else true
      case None => false
    }
  }

  private[server] def closeLastTransactionStreamPartitionDatabases() = {
    scala.util.Try(lastTransactionDatabase.close())
    scala.util.Try(lastCheckpointedTransactionDatabase.close())
  }
}
