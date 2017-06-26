/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDatabaseBatch, KeyValueDatabaseManager}
import com.google.common.cache.Cache


class LastTransactionStreamPartition(rocksMetaServiceDB: KeyValueDatabaseManager) {
  private final val lastTransactionDatabase =
    rocksMetaServiceDB.getDatabase(RocksStorage.LAST_OPENED_TRANSACTION_STORAGE)

  private final val lastCheckpointedTransactionDatabase =
    rocksMetaServiceDB.getDatabase(RocksStorage.LAST_CHECKPOINTED_TRANSACTION_STORAGE)

  private final def fillLastTransactionStreamPartitionTable: Cache[KeyStreamPartition, LastOpenedAndCheckpointedTransaction] = {
    val hoursToLive = 1
    val cache = com.google.common.cache.CacheBuilder.newBuilder()
      .expireAfterAccess(hoursToLive, TimeUnit.HOURS)
      .build[KeyStreamPartition, LastOpenedAndCheckpointedTransaction]()
    cache
  }

  private final val lastTransactionStreamPartitionRamTable: Cache[KeyStreamPartition, LastOpenedAndCheckpointedTransaction] =
    fillLastTransactionStreamPartitionTable

  final def getLastTransactionIDAndCheckpointedID(streamID: Int, partition: Int): Option[LastOpenedAndCheckpointedTransaction] = {
    val key = KeyStreamPartition(streamID, partition)
    Option(lastTransactionStreamPartitionRamTable.getIfPresent(key))
      .orElse {
        val lastOpenedTransaction =
          Option(lastTransactionDatabase.get(key.toByteArray))
            .map(data => TransactionID.fromByteArray(data))

        val lastCheckpointedTransaction =
          Option(lastCheckpointedTransactionDatabase.get(key.toByteArray))
            .map(data => TransactionID.fromByteArray(data))

        lastOpenedTransaction.map(openedTxn =>
          LastOpenedAndCheckpointedTransaction(openedTxn, lastCheckpointedTransaction)
        )
      }
  }

  private val comparator = com.bwsw.tstreamstransactionserver.`implicit`.Implicits.ByteArray
  final def deleteLastOpenedAndCheckpointedTransactions(streamID: Int, batch: KeyValueDatabaseBatch) {
    val from = KeyStreamPartition(streamID, Int.MinValue).toByteArray
    val to = KeyStreamPartition(streamID, Int.MaxValue).toByteArray

    val lastTransactionDatabaseIterator = lastTransactionDatabase.iterator
    lastTransactionDatabaseIterator.seek(from)
    while (
      lastTransactionDatabaseIterator.isValid &&
        comparator.compare(lastTransactionDatabaseIterator.key(), to) <= 0
    ) {
      batch.remove(RocksStorage.LAST_OPENED_TRANSACTION_STORAGE, lastTransactionDatabaseIterator.key())
      lastTransactionDatabaseIterator.next()
    }
    lastTransactionDatabaseIterator.close()

    val lastCheckpointedTransactionDatabaseIterator = lastCheckpointedTransactionDatabase.iterator
    lastCheckpointedTransactionDatabaseIterator.seek(from)
    while (
      lastCheckpointedTransactionDatabaseIterator.isValid &&
      comparator.compare(lastCheckpointedTransactionDatabaseIterator.key(), to) <= 0)
    {
      batch.remove(RocksStorage.LAST_CHECKPOINTED_TRANSACTION_STORAGE, lastCheckpointedTransactionDatabaseIterator.key())
      lastCheckpointedTransactionDatabaseIterator.next()
    }
    lastCheckpointedTransactionDatabaseIterator.close()
  }

  private[transactionMetadataService] final def putLastTransaction(key: KeyStreamPartition,
                                                                   transactionId: Long, isOpenedTransaction: Boolean,
                                                                   batch: KeyValueDatabaseBatch) = {
    val updatedTransactionID = new TransactionID(transactionId)
    if (isOpenedTransaction)
      batch.put(RocksStorage.LAST_OPENED_TRANSACTION_STORAGE, key.toByteArray, updatedTransactionID.toByteArray)
    else
      batch.put(RocksStorage.LAST_CHECKPOINTED_TRANSACTION_STORAGE, key.toByteArray, updatedTransactionID.toByteArray)
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
