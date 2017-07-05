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
package com.bwsw.tstreamstransactionserver.netty.server

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceImpl, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseBatch
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerIDAndItsLastRecordID, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCRUD, StreamServiceImpl}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{LastOpenedAndCheckpointedTransaction, LastTransactionStreamPartition}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService._
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.rpc._

import scala.collection.Set
import scala.collection.mutable.ListBuffer



class TransactionServer(val executionContext: ServerExecutionContextGrids,
                        authOpts: AuthenticationOptions,
                        storageOpts: StorageOptions,
                        rocksStorageOpts: RocksStorageOptions,
                        streamZkDatabase: StreamCRUD)
{
  private val authService = new AuthServiceImpl(authOpts)

  private val rocksStorage = new RocksStorage(
    storageOpts,
    rocksStorageOpts
  )
  private val streamServiceImpl = new StreamServiceImpl(
    streamZkDatabase
  )

  private val transactionIDService =
    com.bwsw.tstreamstransactionserver.netty.server.transactionIDService.TransactionIDService

  private val consumerServiceImpl = new ConsumerServiceImpl(
    rocksStorage.rocksMetaServiceDB
  )
  private val lastTransactionStreamPartition = new LastTransactionStreamPartition(
    rocksStorage.rocksMetaServiceDB
  )
  private[server] val transactionMetaServiceImpl = new TransactionMetaServiceImpl(
    rocksStorage.rocksMetaServiceDB,
    lastTransactionStreamPartition,
    consumerServiceImpl
  )
  private val transactionDataServiceImpl = new TransactionDataServiceImpl(
    storageOpts,
    rocksStorageOpts,
    streamZkDatabase
  )

  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean, func: => Unit): Long =
    transactionMetaServiceImpl.notifyProducerTransactionCompleted(onNotificationCompleted, func)

  final def removeProducerTransactionNotification(id: Long): Boolean =
    transactionMetaServiceImpl.removeProducerTransactionNotification(id)

  final def notifyConsumerTransactionCompleted(onNotificationCompleted: ConsumerTransaction => Boolean, func: => Unit): Long =
    consumerServiceImpl.notifyConsumerTransactionCompleted(onNotificationCompleted, func)

  final def removeConsumerTransactionNotification(id: Long): Boolean =
    consumerServiceImpl.removeConsumerTransactionNotification(id)

  final def getLastProcessedCommitLogFileID: Long =
    transactionMetaServiceImpl.getLastProcessedCommitLogFileID.getOrElse(-1L)

  final def getLastProcessedLedgersAndRecordIDs: Option[Array[LedgerIDAndItsLastRecordID]] =
    transactionMetaServiceImpl.getLastProcessedLedgerAndRecordIDs

  final def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): Int =
    streamServiceImpl.putStream(stream, partitions, description, ttl)

  final def checkStreamExists(name: String): Boolean =
    streamServiceImpl.checkStreamExists(name)

  final def getStream(name: String): Option[rpc.Stream] =
    streamServiceImpl.getStream(name)

  final def delStream(name: String): Boolean =
    streamServiceImpl.delStream(name)

  final def getTransactionID: Long =
    transactionIDService.getTransaction()

  final def getTransactionIDByTimestamp(timestamp: Long): Long =
    transactionIDService.getTransaction(timestamp)

  @throws[StreamDoesNotExist]
  final def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): Boolean =
    transactionDataServiceImpl.putTransactionData(streamID, partition, transaction, data, from)

  final def putTransactions(transactions: Seq[ProducerTransactionRecord],
                            batch: KeyValueDatabaseBatch): ListBuffer[Unit => Unit] = {
    transactionMetaServiceImpl.putTransactions(transactions, batch)
  }

  final def getTransaction(streamID: Int, partition: Int, transaction: Long): TransactionInfo =
    transactionMetaServiceImpl.getTransaction(streamID, partition, transaction)

  final def getOpenedTransaction(key: ProducerTransactionKey): Option[ProducerTransactionValue] =
    transactionMetaServiceImpl.getOpenedTransaction(key)

  final def getLastCheckpointedTransaction(streamID: Int, partition: Int): Option[Long] =
    lastTransactionStreamPartition.getLastTransactionIDAndCheckpointedID(streamID, partition)
      .flatMap(_.checkpointed.map(txn => txn.id)).orElse(Some(-1L))

  final def getLastTransactionIDAndCheckpointedID(streamID: Int, partition: Int): Option[LastOpenedAndCheckpointedTransaction] =
    lastTransactionStreamPartition.getLastTransactionIDAndCheckpointedID(streamID, partition)

  final def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: Set[TransactionStates]): ScanTransactionsInfo =
    transactionMetaServiceImpl.scanTransactions(streamID, partition, from, to, count, states)

  final def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int): Seq[ByteBuffer] = {
    transactionDataServiceImpl.getTransactionData(streamID, partition, transaction, from, to)
  }

  final def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord],
                                    batch: KeyValueDatabaseBatch): ListBuffer[(Unit) => Unit] = {
    consumerServiceImpl.putConsumersCheckpoints(consumerTransactions, batch)
  }

  final def getConsumerState(name: String, streamID: Int, partition: Int): Long = {
    consumerServiceImpl.getConsumerState(name, streamID, partition)
  }

  final def isValid(token: Int): Boolean =
    authService.isValid(token)

  final def authenticate(authKey: String): Int = {
    authService.authenticate(authKey)
  }

  final def getBigCommit(fileID: Long): BigCommit = {
    val key = CommitLogKey(fileID).toByteArray
    new BigCommit(this, RocksStorage.COMMIT_LOG_STORE, key, Array.emptyByteArray)
  }

  final def getBigCommit(processedLastRecordIDsAcrossLedgers: Array[LedgerIDAndItsLastRecordID]): BigCommit = {
    val value = MetadataRecord(processedLastRecordIDsAcrossLedgers).toByteArray
    new BigCommit(this, RocksStorage.BOOKKEEPER_LOG_STORE, BigCommit.bookkeeperKey, value)
  }

  final def getNewBatch: KeyValueDatabaseBatch =
    rocksStorage.newBatch

  final def createAndExecuteTransactionsToDeleteTask(timestamp: Long): Unit =
    transactionMetaServiceImpl.createAndExecuteTransactionsToDeleteTask(timestamp)

  final def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases(): Unit = {
    stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
    closeAllDatabases()
  }

  private[server] final def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    executionContext.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
  }

  final def closeAllDatabases(): Unit = {
    rocksStorage.rocksMetaServiceDB.close()
    transactionDataServiceImpl.closeTransactionDataDatabases()
  }
}