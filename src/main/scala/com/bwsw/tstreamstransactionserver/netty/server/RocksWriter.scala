package com.bwsw.tstreamstransactionserver.netty.server

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceWriter, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import com.bwsw.tstreamstransactionserver.netty.server.storage.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionsCleaner, ProducerStateMachineCache, ProducerTransactionRecord, TransactionMetaServiceWriter}

class RocksWriter(rocksStorage: MultiAndSingleNodeRockStorage,
                  transactionDataService: TransactionDataService) {

  protected val consumerService =
    new ConsumerServiceWriter(
      rocksStorage.getRocksStorage
    )

  protected val producerTransactionsCleaner =
    new ProducerTransactionsCleaner(
      rocksStorage.getRocksStorage
    )

  protected val producerStateMachineCache =
    new ProducerStateMachineCache(rocksStorage.getRocksStorage)

  protected val transactionMetaServiceWriter =
    new TransactionMetaServiceWriter(
      rocksStorage.getRocksStorage,
      producerStateMachineCache
    )

  @throws[StreamDoesNotExist]
  final def putTransactionData(streamID: Int,
                               partition: Int,
                               transaction: Long,
                               data: Seq[ByteBuffer],
                               from: Int): Boolean =
    transactionDataService.putTransactionData(
      streamID,
      partition,
      transaction,
      data,
      from
    )

  final def putTransactions(transactions: Seq[ProducerTransactionRecord],
                            batch: KeyValueDbBatch): Unit = {
    transactionMetaServiceWriter.putTransactions(
      transactions,
      batch
    )
  }

  final def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord],
                                    batch: KeyValueDbBatch): Unit = {
    consumerService.putConsumersCheckpoints(consumerTransactions, batch)
  }

  final def getNewBatch: KeyValueDbBatch =
    rocksStorage.newBatch

  final def createAndExecuteTransactionsToDeleteTask(timestamp: Long): Unit =
    producerTransactionsCleaner
      .cleanExpiredProducerTransactions(timestamp)

  def clearProducerTransactionCache(): Unit =
    producerStateMachineCache.clear()
}
