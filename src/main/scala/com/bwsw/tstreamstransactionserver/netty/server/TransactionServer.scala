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

import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceImpl, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseBatch
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerIDAndItsLastRecordID, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.storage.{AllInOneRockStorage, RocksStorage}
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamRepository, StreamServiceImpl}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{LastOpenedAndCheckpointedTransaction, LastTransactionStreamPartition}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService._
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.rpc._

import scala.collection.Set
import scala.collection.mutable.ListBuffer



class TransactionServer(authOpts: AuthenticationOptions,
                        streamRepository: StreamRepository,
                        rocksWriter: RocksWriter,
                        rocksReader: RocksReader)
{
  private val authService = new AuthServiceImpl(authOpts)

  private val streamServiceImpl = new StreamServiceImpl(
    streamRepository
  )

  private val transactionIDService =
    com.bwsw.tstreamstransactionserver.netty.server.transactionIDService.TransactionIdService


  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean, func: => Unit): Long =
    rocksWriter.notifyProducerTransactionCompleted(onNotificationCompleted, func)

  final def removeProducerTransactionNotification(id: Long): Boolean =
    rocksWriter.removeProducerTransactionNotification(id)

  final def notifyConsumerTransactionCompleted(onNotificationCompleted: ConsumerTransaction => Boolean, func: => Unit): Long =
    rocksWriter.notifyConsumerTransactionCompleted(onNotificationCompleted, func)

  final def removeConsumerTransactionNotification(id: Long): Boolean =
    rocksWriter.removeConsumerTransactionNotification(id)

  final def getLastProcessedCommitLogFileID: Long =
    rocksReader.getLastProcessedCommitLogFileID

  final def getLastProcessedLedgersAndRecordIDs: Option[Array[LedgerIDAndItsLastRecordID]] =
    rocksReader.getLastProcessedLedgersAndRecordIDs

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
    rocksWriter.putTransactionData(streamID, partition, transaction, data, from)

  final def putTransactions(transactions: Seq[ProducerTransactionRecord],
                            batch: KeyValueDatabaseBatch): ListBuffer[Unit => Unit] = {
    rocksWriter.putTransactions(transactions, batch)
  }

  final def getTransaction(streamID: Int, partition: Int, transaction: Long): TransactionInfo =
    rocksReader.getTransaction(streamID, partition, transaction)

  final def getOpenedTransaction(key: ProducerTransactionKey): Option[ProducerTransactionValue] =
    rocksReader.getOpenedTransaction(key)

  final def getLastCheckpointedTransaction(streamID: Int, partition: Int): Option[Long] =
    rocksReader.getLastTransactionIDAndCheckpointedID(streamID, partition)
      .flatMap(_.checkpointed.map(txn => txn.id)).orElse(Some(-1L))

  final def getLastTransactionIDAndCheckpointedID(streamID: Int, partition: Int): Option[LastOpenedAndCheckpointedTransaction] =
    rocksReader.getLastTransactionIDAndCheckpointedID(streamID, partition)

  final def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: Set[TransactionStates]): ScanTransactionsInfo =
    rocksReader.scanTransactions(streamID, partition, from, to, count, states)

  final def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int): Seq[ByteBuffer] = {
    rocksReader.getTransactionData(streamID, partition, transaction, from, to)
  }

  final def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord],
                                    batch: KeyValueDatabaseBatch): ListBuffer[(Unit) => Unit] = {
    rocksWriter.putConsumersCheckpoints(consumerTransactions, batch)
  }

  final def getConsumerState(name: String, streamID: Int, partition: Int): Long = {
    rocksReader.getConsumerState(name, streamID, partition)
  }

  final def isValid(token: Int): Boolean =
    authService.isValid(token)

  final def authenticate(authKey: String): Int = {
    authService.authenticate(authKey)
  }

  final def getBigCommit(fileID: Long): BigCommit =
    rocksWriter.getBigCommit(fileID)

  final def getBigCommit(processedLastRecordIDsAcrossLedgers: Array[LedgerIDAndItsLastRecordID]): BigCommit =
    rocksWriter.getBigCommit(processedLastRecordIDsAcrossLedgers)

  final def getNewBatch: KeyValueDatabaseBatch =
    rocksWriter.getNewBatch

  final def createAndExecuteTransactionsToDeleteTask(timestamp: Long): Unit =
    rocksWriter.createAndExecuteTransactionsToDeleteTask(timestamp)
}