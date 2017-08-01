package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceWriter
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.test.TestConsumerServiceWriter
import com.bwsw.tstreamstransactionserver.netty.server.storage.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.test.{TestProducerTransactionsCleaner, TestTransactionMetaServiceWriter}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionsCleaner, TransactionMetaServiceWriter}
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}

class TestRocksWriter(rocksStorage: MultiAndSingleNodeRockStorage,
                      transactionDataService: TransactionDataService,
                      producerTransactionNotifier: Notifier[ProducerTransaction],
                      consumerTransactionNotifier: Notifier[ConsumerTransaction])
  extends RocksWriter(
    rocksStorage,
    transactionDataService) {

  override protected val consumerService: ConsumerServiceWriter =
    new TestConsumerServiceWriter(
      rocksStorage.getRocksStorage,
      consumerTransactionNotifier
    )

  override protected val producerTransactionsCleaner: ProducerTransactionsCleaner =
    new TestProducerTransactionsCleaner(
      rocksStorage.getRocksStorage,
      producerTransactionNotifier
    )

  override protected val transactionMetaServiceWriter: TransactionMetaServiceWriter =
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
