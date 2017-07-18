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
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamRepository, StreamServiceImpl}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{LastOpenedAndCheckpointedTransaction, LastTransactionStreamPartition}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionValue, TransactionMetaServiceImpl}
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.rpc._

import scala.collection.Set



class TransactionServer(authOpts: AuthenticationOptions,
                        storageOpts: StorageOptions,
                        rocksStorageOpts: RocksStorageOptions,
                        streamCache: StreamRepository)
{
  private val authService = new AuthServiceImpl(authOpts)

  private val rocksStorage = new RocksStorage(
    storageOpts,
    rocksStorageOpts
  )
  private val streamServiceImpl = new StreamServiceImpl(
    streamCache
  )

  private val transactionIDService =
    com.bwsw.tstreamstransactionserver.netty.server.transactionIDService.TransactionIdService

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
    streamCache
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

  final def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): Boolean =
    transactionDataServiceImpl.putTransactionData(streamID, partition, transaction, data, from)

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

  final def getConsumerState(name: String, streamID: Int, partition: Int): Long = {
    consumerServiceImpl.getConsumerState(name, streamID, partition)
  }

  final def isValid(token: Int): Boolean =
    authService.isValid(token)

  final def authenticate(authKey: String): Int = {
    authService.authenticate(authKey)
  }

  final def getBigCommit(fileID: Long): transactionMetaServiceImpl.BigCommit =
    transactionMetaServiceImpl.getBigCommit(fileID)

  final def createAndExecuteTransactionsToDeleteTask(timestamp: Long): Unit =
    transactionMetaServiceImpl.createAndExecuteTransactionsToDeleteTask(timestamp)

  final def closeAllDatabases(): Unit = {
    rocksStorage.rocksMetaServiceDB.close()
    transactionDataServiceImpl.closeTransactionDataDatabases()
  }
}