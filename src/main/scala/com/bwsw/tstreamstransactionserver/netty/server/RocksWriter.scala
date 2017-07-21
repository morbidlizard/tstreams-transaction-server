package com.bwsw.tstreamstransactionserver.netty.server

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceImpl, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseBatch
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerIDAndItsLastRecordID, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.storage.{AllInOneRockStorage, RocksStorage}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{CommitLogKey, ProducerTransactionRecord, TransactionMetaServiceImpl}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.LastTransactionStreamPartition
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}

import scala.collection.mutable.ListBuffer

class RocksWriter(rocksStorage: AllInOneRockStorage,
                  lastTransactionStreamPartition: LastTransactionStreamPartition,
                  transactionDataService: TransactionDataServiceImpl) {

  private val consumerServiceImpl = new ConsumerServiceImpl(
    rocksStorage.getRocksStorage
  )

  private val transactionMetaServiceImpl = new TransactionMetaServiceImpl(
    rocksStorage.getRocksStorage,
    lastTransactionStreamPartition,
    consumerServiceImpl
  )


  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean, func: => Unit): Long =
    transactionMetaServiceImpl.notifyProducerTransactionCompleted(onNotificationCompleted, func)

  final def removeProducerTransactionNotification(id: Long): Boolean =
    transactionMetaServiceImpl.removeProducerTransactionNotification(id)

  final def notifyConsumerTransactionCompleted(onNotificationCompleted: ConsumerTransaction => Boolean, func: => Unit): Long =
    consumerServiceImpl.notifyConsumerTransactionCompleted(onNotificationCompleted, func)

  final def removeConsumerTransactionNotification(id: Long): Boolean =
    consumerServiceImpl.removeConsumerTransactionNotification(id)

  @throws[StreamDoesNotExist]
  final def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): Boolean =
    transactionDataService.putTransactionData(streamID, partition, transaction, data, from)

  final def putTransactions(transactions: Seq[ProducerTransactionRecord],
                            batch: KeyValueDatabaseBatch): ListBuffer[Unit => Unit] = {
    transactionMetaServiceImpl.putTransactions(transactions, batch)
  }


  final def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord],
                                    batch: KeyValueDatabaseBatch): ListBuffer[(Unit) => Unit] = {
    consumerServiceImpl.putConsumersCheckpoints(consumerTransactions, batch)
  }

  final def getConsumerState(name: String, streamID: Int, partition: Int): Long = {
    consumerServiceImpl.getConsumerState(name, streamID, partition)
  }

  final def getBigCommit(fileID: Long): BigCommit = {
    val value = CommitLogKey(fileID).toByteArray
    new BigCommit(this, RocksStorage.COMMIT_LOG_STORE, BigCommit.commitLogKey, value)
  }

  final def getBigCommit(processedLastRecordIDsAcrossLedgers: Array[LedgerIDAndItsLastRecordID]): BigCommit = {
    val value = MetadataRecord(processedLastRecordIDsAcrossLedgers).toByteArray
    new BigCommit(this, RocksStorage.BOOKKEEPER_LOG_STORE, BigCommit.bookkeeperKey, value)
  }

  final def getNewBatch: KeyValueDatabaseBatch =
    rocksStorage.newBatch

  final def createAndExecuteTransactionsToDeleteTask(timestamp: Long): Unit =
    transactionMetaServiceImpl.createAndExecuteTransactionsToDeleteTask(timestamp)
}
