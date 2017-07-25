package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService


import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDatabaseBatch, KeyValueDatabaseManager}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{KeyStreamPartition, LastOpenedAndCheckpointedTransaction, LastTransactionReader, TransactionID}

import scala.collection.mutable

private[server] final class ProducerStateMachine(rocksDB: KeyValueDatabaseManager) {
  private val producerTransactionsWithOpenedStateDatabase =
    rocksDB.getDatabase(RocksStorage.TRANSACTION_OPEN_STORE)

  private val transactionsRamTable =
    mutable.Map.empty[ProducerTransactionKey, ProducerTransactionValue]

  private val lastTransactionStreamPartitionRamTable =
    mutable.Map.empty[KeyStreamPartition, LastOpenedAndCheckpointedTransaction]


  def getProducerTransaction(key: ProducerTransactionKey): Option[ProducerTransactionValue] = {
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

  def updateProducerTransaction(key: ProducerTransactionKey,
                                value: ProducerTransactionValue): Unit = {
    transactionsRamTable.put(key, value)
  }

  def removeProducerTransaction(key: ProducerTransactionKey): Unit = {
    transactionsRamTable -= key
  }


  //  private val comparator = com.bwsw.tstreamstransactionserver.`implicit`.Implicits.ByteArray
  //  final def deleteLastOpenedAndCheckpointedTransactions(streamID: Int, batch: KeyValueDatabaseBatch) {
  //    val from = KeyStreamPartition(streamID, Int.MinValue).toByteArray
  //    val to = KeyStreamPartition(streamID, Int.MaxValue).toByteArray
  //
  //    val lastTransactionDatabaseIterator = lastTransactionDatabase.iterator
  //    lastTransactionDatabaseIterator.seek(from)
  //    while (
  //      lastTransactionDatabaseIterator.isValid &&
  //        comparator.compare(lastTransactionDatabaseIterator.key(), to) <= 0
  //    ) {
  //      batch.remove(RocksStorage.LAST_OPENED_TRANSACTION_STORAGE, lastTransactionDatabaseIterator.key())
  //      lastTransactionDatabaseIterator.next()
  //    }
  //    lastTransactionDatabaseIterator.close()
  //
  //    val lastCheckpointedTransactionDatabaseIterator = lastCheckpointedTransactionDatabase.iterator
  //    lastCheckpointedTransactionDatabaseIterator.seek(from)
  //    while (
  //      lastCheckpointedTransactionDatabaseIterator.isValid &&
  //      comparator.compare(lastCheckpointedTransactionDatabaseIterator.key(), to) <= 0)
  //    {
  //      batch.remove(RocksStorage.LAST_CHECKPOINTED_TRANSACTION_STORAGE, lastCheckpointedTransactionDatabaseIterator.key())
  //      lastCheckpointedTransactionDatabaseIterator.next()
  //    }
  //    lastCheckpointedTransactionDatabaseIterator.close()
  //  }

  private val lastTransactionReader =
    new LastTransactionReader(rocksDB)

  def getLastTransactionIDAndCheckpointedID(streamID: Int,
                                            partition: Int): Option[LastOpenedAndCheckpointedTransaction] = {
    val key = KeyStreamPartition(streamID, partition)
    lastTransactionStreamPartitionRamTable
      .get(key)
      .orElse(
        lastTransactionReader
          .getLastTransactionIDAndCheckpointedID(streamID, partition)
      )
  }

  def putLastTransaction(key: KeyStreamPartition,
                         transactionId: Long,
                         isOpenedTransaction: Boolean,
                         batch: KeyValueDatabaseBatch): Boolean = {
    val updatedTransactionID = new TransactionID(transactionId)
    if (isOpenedTransaction)
      batch.put(RocksStorage.LAST_OPENED_TRANSACTION_STORAGE,
        key.toByteArray,
        updatedTransactionID.toByteArray
      )
    else
      batch.put(RocksStorage.LAST_CHECKPOINTED_TRANSACTION_STORAGE,
        key.toByteArray,
        updatedTransactionID.toByteArray
      )
  }

  def updateLastTransactionStreamPartitionRamTable(key: KeyStreamPartition,
                                                   transaction: Long,
                                                   isOpenedTransaction: Boolean): Unit = {
    val lastOpenedAndCheckpointedTransaction =
      lastTransactionStreamPartitionRamTable.get(key)
    lastOpenedAndCheckpointedTransaction match {
      case Some(x) =>
        if (isOpenedTransaction)
          lastTransactionStreamPartitionRamTable.put(
            key,
            LastOpenedAndCheckpointedTransaction(
              TransactionID(transaction),
              x.checkpointed
            )
          )
        else
          lastTransactionStreamPartitionRamTable.put(
            key,
            LastOpenedAndCheckpointedTransaction(
              x.opened,
              Some(TransactionID(transaction))
            )
          )
      case None if isOpenedTransaction =>
        lastTransactionStreamPartitionRamTable.put(
          key,
          LastOpenedAndCheckpointedTransaction(
            TransactionID(transaction),
            None
          )
        )
      case _ => //do nothing
    }
  }

  def isThatTransactionOutOfOrder(key: KeyStreamPartition,
                                  transactionThatId: Long): Boolean = {
    val lastTransactionOpt = lastTransactionStreamPartitionRamTable.get(key)
    lastTransactionOpt match {
      case Some(transactionId) =>
        if (transactionId.opened.id < transactionThatId)
          false
        else
          true
      case None =>
        false
    }
  }

  def clear(): Unit = {
    lastTransactionStreamPartitionRamTable.clear()
    transactionsRamTable.clear()
  }
}
