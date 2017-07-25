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
package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDatabaseBatch, KeyValueDatabaseManager}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler._
import com.bwsw.tstreamstransactionserver.rpc._
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, ListBuffer}



class TransactionMetaServiceImpl(rocksDB: KeyValueDatabaseManager,
                                 producerStateMachine: ProducerStateMachine)
{
  import producerStateMachine._

  val notifier = new ProducerTransactionStateNotifier()

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val producerTransactionsWithOpenedStateDatabase =
    rocksDB.getDatabase(RocksStorage.TRANSACTION_OPEN_STORE)


  private final def selectInOrderProducerTransactions(transactions: Seq[ProducerTransactionRecord],
                                                                       batch: KeyValueDatabaseBatch) = {
    val producerTransactions = ArrayBuffer[ProducerTransactionRecord]()
    transactions foreach { txn =>
      val key = KeyStreamPartition(txn.stream, txn.partition)
      if (txn.state != TransactionStates.Opened) {
        producerTransactions += txn
      } else if (!isThatTransactionOutOfOrder(key, txn.transactionID)) {
        // updating RAM table, and last opened transaction database.
        updateLastTransactionStreamPartitionRamTable(
          key,
          txn.transactionID,
          isOpenedTransaction = true
        )

        putLastTransaction(key,
          txn.transactionID,
          isOpenedTransaction = true,
          batch
        )

        if (logger.isDebugEnabled)
          logger.debug(
            s"On stream:${key.stream} partition:${key.partition} " +
              s"last opened transaction is ${txn.transactionID} now."
          )
        producerTransactions += txn
      }
    }
    producerTransactions
  }


  private final def groupProducerTransactionsByStreamPartitionTransactionID(producerTransactions: Seq[ProducerTransactionRecord]) =
    producerTransactions.groupBy(txn => txn.key)

  private final def updateLastCheckpointedTransactionAndPutToDatabase(key: stateHandler.KeyStreamPartition,
                                                                      producerTransactionWithNewState: ProducerTransactionRecord,
                                                                      batch: KeyValueDatabaseBatch): Unit = {
    updateLastTransactionStreamPartitionRamTable(
      key,
      producerTransactionWithNewState.transactionID,
      isOpenedTransaction = false
    )

    putLastTransaction(
      key,
      producerTransactionWithNewState.transactionID,
      isOpenedTransaction = false,
      batch
    )

    if (logger.isDebugEnabled())
      logger.debug(
        s"On stream:${key.stream} partition:${key.partition} " +
        s"last checkpointed transaction is ${producerTransactionWithNewState.transactionID} now."
      )
  }

  private def putTransactionToAllAndOpenedTables(producerTransactionRecord: ProducerTransactionRecord,
                                                 batch: KeyValueDatabaseBatch) =
  {

    if (producerTransactionRecord.state == TransactionStates.Checkpointed) {
      updateLastCheckpointedTransactionAndPutToDatabase(
        stateHandler.KeyStreamPartition(
          producerTransactionRecord.stream,
          producerTransactionRecord.partition
        ),
        producerTransactionRecord,
        batch
      )
    }

    val binaryTxn = producerTransactionRecord.producerTransaction.toByteArray
    val binaryKey = producerTransactionRecord.key.toByteArray

    producerStateMachine.updateProducerTransaction(
      producerTransactionRecord.key,
      producerTransactionRecord.producerTransaction
    )

    if (producerTransactionRecord.state == TransactionStates.Opened) {
      batch.put(RocksStorage.TRANSACTION_OPEN_STORE, binaryKey, binaryTxn)
    }
    else {
      batch.remove(RocksStorage.TRANSACTION_OPEN_STORE, binaryKey)
    }
    batch.put(RocksStorage.TRANSACTION_ALL_STORE, binaryKey, binaryTxn)

    if (logger.isDebugEnabled)
      logger.debug(s"Producer transaction on stream: ${producerTransactionRecord.stream}" +
        s"partition ${producerTransactionRecord.partition}, transactionId ${producerTransactionRecord.transactionID} " +
        s"with state ${producerTransactionRecord.state} is ready for commit"
    )
  }


  def putTransactions(transactions: Seq[ProducerTransactionRecord],
                      batch: KeyValueDatabaseBatch): ListBuffer[Unit => Unit] = {

    val notifications = new scala.collection.mutable.ListBuffer[Unit => Unit]()

    val producerTransactions =
      selectInOrderProducerTransactions(transactions, batch)

    val groupedProducerTransactions =
      groupProducerTransactionsByStreamPartitionTransactionID(producerTransactions)

    groupedProducerTransactions.foreach { case (key, txns) =>
      //retrieving an opened transaction from opened transaction database if it exist
      val openedTransactionOpt = producerStateMachine.getProducerTransaction(key)

      val orderedTxns = txns.sorted
      val transactionsToProcess = openedTransactionOpt
        .map(data =>
          ProducerTransactionRecord(key, data) +: orderedTxns
        )
        .getOrElse(orderedTxns)

      val finalStateOpt = ProducerTransactionState
        .transiteTransactionsToFinalState(
          transactionsToProcess,
          transaction => {
            if (notifier.areThereAnyProducerNotifies)
              notifications += notifier.tryCompleteProducerNotify(transaction)
          }
        )

      finalStateOpt
        .filter(ProducerTransactionState.checkFinalStateBeStoredInDB)
        .foreach { finalState =>
          val transaction = finalState.producerTransactionRecord
          putTransactionToAllAndOpenedTables(transaction, batch)
        }
    }

    notifications
  }

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

    val notifications = new ListBuffer[Unit => Unit]()
    while (iterator.isValid) {
      val producerTransactionValue = ProducerTransactionValue.fromByteArray(iterator.value())
      if (doesProducerTransactionExpired(producerTransactionValue)) {
        if (logger.isDebugEnabled)
          logger.debug(s"Cleaning $producerTransactionValue as it's expired.")

        val producerTransactionValueTimestampUpdated = producerTransactionValue.copy(timestamp = timestampToDeleteTransactions)
        val key = iterator.key()
        val producerTransactionKey = ProducerTransactionKey.fromByteArray(key)

        val canceledTransactionRecordDueExpiration =
          TransactionStateHandler.transitProducerTransactionToInvalidState(ProducerTransactionRecord(producerTransactionKey, producerTransactionValueTimestampUpdated))
        if (notifier.areThereAnyProducerNotifies)
          notifications += notifier.tryCompleteProducerNotify(ProducerTransactionRecord(producerTransactionKey, canceledTransactionRecordDueExpiration.producerTransaction))

        batch.put(RocksStorage.TRANSACTION_ALL_STORE, key, canceledTransactionRecordDueExpiration.producerTransaction.toByteArray)

        batch.remove(RocksStorage.TRANSACTION_OPEN_STORE, key)
      }
      iterator.next()
    }
    iterator.close()
    batch.write()

    notifications.foreach(notification => notification(()))
  }

  final def createAndExecuteTransactionsToDeleteTask(timestampToDeleteTransactions: Long): Unit = {
    transactionsToDeleteTask(timestampToDeleteTransactions)
  }
}
