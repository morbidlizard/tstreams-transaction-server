package com.bwsw.tstreamstransactionserver.netty.server

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCache, StreamServiceImpl}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{LastOpenedAndCheckpointedTransaction, LastTransactionStreamPartition}
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.rpc._

import scala.collection.Set



class TransactionServer(val executionContext: ServerExecutionContext,
                        authOpts: AuthOptions,
                        storageOpts: StorageOptions,
                        rocksStorageOpts: RocksStorageOptions,
                        streamCache: StreamCache,
                        timer: Time = new Time{}
                       )
{
  private val authService = new AuthServiceImpl(authOpts)

  private val rocksStorage = new RocksStorage(
    storageOpts
  )
  private val streamServiceImpl = new StreamServiceImpl(
    streamCache
  )
  private val consumerServiceImpl = new ConsumerServiceImpl(
    rocksStorage.rocksMetaServiceDB,
    streamCache
  )
  private val lastTransactionStreamPartition = new LastTransactionStreamPartition(
    rocksStorage.rocksMetaServiceDB
  )
  private[server] val transactionMetaServiceImpl = new TransactionMetaServiceImpl(
    rocksStorage.rocksMetaServiceDB,
    streamCache,
    lastTransactionStreamPartition,
    consumerServiceImpl
  )
  private val transactionDataServiceImpl = new TransactionDataServiceImpl(
    storageOpts,
    rocksStorageOpts,
    streamCache
  )

  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean, func: => Unit): Long =
    transactionMetaServiceImpl.notifyProducerTransactionCompleted(onNotificationCompleted, Boolean)

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

  final def checkStreamExists(streamID: Int): Boolean =
    streamServiceImpl.checkStreamExists(streamID)

  final def getStream(streamID: Int): rpc.Stream =
    streamServiceImpl.getStream(streamID)

  final def delStream(streamID: Int): Boolean =
    streamServiceImpl.delStream(streamID)

  final def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): Boolean =
    transactionDataServiceImpl.putTransactionData(streamID, partition, transaction, data, from)

  final def getTransaction(streamID: Int, partition: Int, transaction: Long): TransactionInfo =
    transactionMetaServiceImpl.getTransaction(streamID, partition, transaction)

  final def getLastCheckpointedTransaction(streamID: Int, partition: Int): Option[Long] =
    lastTransactionStreamPartition.getLastTransactionIDAndCheckpointedID(streamID, partition)
      .flatMap(_.checkpointed.map(txn => txn.id)).orElse(Some(-1L))

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

  final def getBigCommit(flieID: Long): transactionMetaServiceImpl.BigCommit =
    transactionMetaServiceImpl.getBigCommit(flieID)

  final def createAndExecuteTransactionsToDeleteTask(timestamp: Long): Unit =
    transactionMetaServiceImpl.createAndExecuteTransactionsToDeleteTask(timestamp)

  final def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases(): Unit = {
    stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
    closeAllDatabases()
  }

  final def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    executionContext.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
  }

  final def closeAllDatabases(): Unit = {
    rocksStorage.rocksMetaServiceDB.close()
    transactionDataServiceImpl.closeTransactionDataDatabases()
  }
}