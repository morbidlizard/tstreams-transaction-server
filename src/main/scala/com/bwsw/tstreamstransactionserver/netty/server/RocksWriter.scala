package com.bwsw.tstreamstransactionserver.netty.server

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceImpl, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseBatch
import com.bwsw.tstreamstransactionserver.netty.server.storage.AllInOneRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionsCleaner, ProducerStateMachineCache, ProducerTransactionRecord, TransactionMetaServiceImpl}

class RocksWriter(rocksStorage: AllInOneRockStorage,
                  transactionDataService: TransactionDataServiceImpl) {

  protected val consumerServiceImpl = new ConsumerServiceImpl(
    rocksStorage.getRocksStorage
  )

  protected val cleaner = new ProducerTransactionsCleaner(
    rocksStorage.getRocksStorage
  )

  protected val producerStateMachineCache =
    new ProducerStateMachineCache(rocksStorage.getRocksStorage)

  protected val transactionMetaServiceImpl = new TransactionMetaServiceImpl(
    rocksStorage.getRocksStorage,
    producerStateMachineCache
  )

  @throws[StreamDoesNotExist]
  final def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): Boolean =
    transactionDataService.putTransactionData(streamID, partition, transaction, data, from)

  final def putTransactions(transactions: Seq[ProducerTransactionRecord],
                            batch: KeyValueDatabaseBatch): Unit = {
    transactionMetaServiceImpl.putTransactions(
      transactions,
      batch
    )
  }

  final def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord],
                                    batch: KeyValueDatabaseBatch): Unit = {
    consumerServiceImpl.putConsumersCheckpoints(consumerTransactions, batch)
  }

  final def getNewBatch: KeyValueDatabaseBatch =
    rocksStorage.newBatch

  final def createAndExecuteTransactionsToDeleteTask(timestamp: Long): Unit =
    cleaner.createAndExecuteTransactionsToDeleteTask(timestamp)

  def clearProducerTransactionCache(): Unit =
    producerStateMachineCache.clear()
}
