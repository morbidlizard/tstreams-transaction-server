package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.test.TestConsumerServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.storage.AllInOneRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.test.{TestProducerTransactionsCleaner, TestTransactionMetaServiceImpl}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionsCleaner, TransactionMetaServiceImpl}
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, ConsumerTransaction}

class TestRocksWriter(rocksStorage: AllInOneRockStorage,
                      transactionDataService: TransactionDataServiceImpl,
                      producerTransactionNotifier: StateNotifier[ProducerTransaction],
                      consumerTransactionNotifier: StateNotifier[ConsumerTransaction])
  extends RocksWriter(
    rocksStorage,
    transactionDataService) {

  override protected val consumerServiceImpl: ConsumerServiceImpl =
    new TestConsumerServiceImpl(
      rocksStorage.getRocksStorage,
      consumerTransactionNotifier
    )

  override protected val cleaner: ProducerTransactionsCleaner =
    new TestProducerTransactionsCleaner(
      rocksStorage.getRocksStorage,
      producerTransactionNotifier
    )

  override protected val transactionMetaServiceImpl: TransactionMetaServiceImpl =
    new TestTransactionMetaServiceImpl(
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
