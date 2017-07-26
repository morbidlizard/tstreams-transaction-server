package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceWriter
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.test.TestConsumerServiceWriter
import com.bwsw.tstreamstransactionserver.netty.server.storage.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.test.{TestProducerTransactionsCleaner, TestTransactionMetaServiceWriter}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionsCleaner, TransactionMetaServiceWriter}
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, ConsumerTransaction}

class TestRocksWriter(rocksStorage: MultiAndSingleNodeRockStorage,
                      transactionDataService: TransactionDataService,
                      producerTransactionNotifier: StateNotifier[ProducerTransaction],
                      consumerTransactionNotifier: StateNotifier[ConsumerTransaction])
  extends RocksWriter(
    rocksStorage,
    transactionDataService) {

  override protected val consumerServiceImpl: ConsumerServiceWriter =
    new TestConsumerServiceWriter(
      rocksStorage.getRocksStorage,
      consumerTransactionNotifier
    )

  override protected val producerTransactionsCleaner: ProducerTransactionsCleaner =
    new TestProducerTransactionsCleaner(
      rocksStorage.getRocksStorage,
      producerTransactionNotifier
    )

  override protected val transactionMetaServiceWriterImpl: TransactionMetaServiceWriter =
    new TestTransactionMetaServiceWriter(
      rocksStorage.getRocksStorage,
      producerStateMachineCache,
      producerTransactionNotifier
    )

  override def clearProducerTransactionCache(): Unit = {
    producerTransactionNotifier.broadcastNotifications()
    consumerTransactionNotifier.broadcastNotifications()
    super.clearProducerTransactionCache()
  }
}
