package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import java.util.concurrent.{Callable, TimeUnit}

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.HasEnvironment
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{Batch, RocksDBALL}
import com.google.common.cache.Cache

trait LastTransactionStreamPartition {
  val executionContext: ServerExecutionContext
  val rocksMetaServiceDB: RocksDBALL

  private final val lastTransactionDatabase = rocksMetaServiceDB.getDatabase(HasEnvironment.LAST_OPENED_TRANSACTION_STORAGE)
  private final val lastCheckpointedTransactionDatabase = rocksMetaServiceDB.getDatabase(HasEnvironment.LAST_CHECKPOINTED_TRANSACTION_STORAGE)


  private final def fillLastTransactionStreamPartitionTable: Cache[KeyStreamPartition, LastOpenedAndCheckpointedTransaction] = {
    val hoursToLive = 1
    val cache = com.google.common.cache.CacheBuilder.newBuilder()
      .expireAfterAccess(hoursToLive, TimeUnit.HOURS)
      .build[KeyStreamPartition, LastOpenedAndCheckpointedTransaction]()


    val lastTransactionDatabaseIterator = lastTransactionDatabase.iterator
    lastTransactionDatabaseIterator.seekToFirst()
    while (lastTransactionDatabaseIterator.isValid) {
      val keyFound = lastTransactionDatabaseIterator.key()
      val dataFound = lastTransactionDatabaseIterator.value()
      cache.put(KeyStreamPartition.fromByteArray(keyFound), LastOpenedAndCheckpointedTransaction(TransactionID.fromByteArray(dataFound), None))
      lastTransactionDatabaseIterator.next()
    }
    lastTransactionDatabaseIterator.close()

    val lastCheckpointedTransactionDatabaseIterator = lastCheckpointedTransactionDatabase.iterator
    lastCheckpointedTransactionDatabaseIterator.seekToFirst()
    while (lastCheckpointedTransactionDatabaseIterator.isValid) {
      val keyFound = lastCheckpointedTransactionDatabaseIterator.key()
      val dataFound = lastCheckpointedTransactionDatabaseIterator.value()
      val lastOpenedAndCheckpointedOpt = Option(cache.getIfPresent(KeyStreamPartition.fromByteArray(keyFound)))
      lastOpenedAndCheckpointedOpt foreach { x =>
        cache.put(
          KeyStreamPartition.fromByteArray(keyFound),
          LastOpenedAndCheckpointedTransaction(x.opened, Some(TransactionID.fromByteArray(dataFound)))
        )
      }
      lastCheckpointedTransactionDatabaseIterator.next()
    }
    lastCheckpointedTransactionDatabaseIterator.close()
    cache
  }

  private final val lastTransactionStreamPartitionRamTable: Cache[KeyStreamPartition, LastOpenedAndCheckpointedTransaction] = fillLastTransactionStreamPartitionTable

  final def getLastTransactionIDAndCheckpointedID(stream: Long, partition: Int): Option[LastOpenedAndCheckpointedTransaction] = {
    val key = KeyStreamPartition(stream, partition)
    val lastTransactionOpt = Option(lastTransactionStreamPartitionRamTable.getIfPresent(key))
    lastTransactionOpt.flatMap{_ =>
      val binaryKey = key.toByteArray
      Option(lastTransactionDatabase.get(binaryKey)).flatMap{dataFound =>
        val openedTransaction =TransactionID.fromByteArray(dataFound)
        LastOpenedAndCheckpointedTransaction(openedTransaction, None)

        Option(lastCheckpointedTransactionDatabase.get(binaryKey)).map{dataFound =>
          val lastCheckpointed = TransactionID.fromByteArray(dataFound)
          LastOpenedAndCheckpointedTransaction(openedTransaction, Some(lastCheckpointed))
        }
      }
    }
  }

  private val comparator = com.bwsw.tstreamstransactionserver.`implicit`.Implicits.ByteArray
  private class DeleteLastOpenedAndCheckpointedTransactions(stream: Long, batch: Batch) extends Callable[Unit] {
    override def call(): Unit = {
      val from = KeyStreamPartition(stream, Int.MinValue).toByteArray
      val to = KeyStreamPartition(stream, Int.MaxValue).toByteArray

      val lastTransactionDatabaseIterator = lastTransactionDatabase.iterator
      lastTransactionDatabaseIterator.seek(from)
      while (lastTransactionDatabaseIterator.isValid && comparator.compare(lastTransactionDatabaseIterator.key(), to) <= 0) {
        batch.remove(HasEnvironment.LAST_OPENED_TRANSACTION_STORAGE, lastTransactionDatabaseIterator.key())
        lastTransactionDatabaseIterator.next()
      }
      lastTransactionDatabaseIterator.close()

      val lastCheckpointedTransactionDatabaseIterator = lastCheckpointedTransactionDatabase.iterator
      lastCheckpointedTransactionDatabaseIterator.seek(from)
      while (lastCheckpointedTransactionDatabaseIterator.isValid && comparator.compare(lastCheckpointedTransactionDatabaseIterator.key(), to) <= 0) {
        batch.remove(HasEnvironment.LAST_CHECKPOINTED_TRANSACTION_STORAGE, lastCheckpointedTransactionDatabaseIterator.key())
        lastCheckpointedTransactionDatabaseIterator.next()
      }
      lastCheckpointedTransactionDatabaseIterator.close()
    }
  }

  final def deleteLastOpenedAndCheckpointedTransactions(stream: Long, batch: Batch): Unit = {
    executionContext.berkeleyWriteContext.submit(new DeleteLastOpenedAndCheckpointedTransactions(stream, batch)).get()
  }

  private[transactionMetadataService] final def putLastTransaction(key: KeyStreamPartition, transactionId: Long, isOpenedTransaction: Boolean, batch: Batch) = {
    val updatedTransactionID = new TransactionID(transactionId)
    if (isOpenedTransaction)
      batch.put(HasEnvironment.LAST_OPENED_TRANSACTION_STORAGE, key.toByteArray, updatedTransactionID.toByteArray)
    else
      batch.put(HasEnvironment.LAST_CHECKPOINTED_TRANSACTION_STORAGE, key.toByteArray, updatedTransactionID.toByteArray)
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
}
