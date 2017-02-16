package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetaService.TransactionMetaServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.сonsumerService.{ConsumerServiceImpl, ConsumerTransactionKey}
import com.bwsw.tstreamstransactionserver.options._
import com.sleepycat.je.{Environment, Transaction}
import transactionService.rpc.ConsumerTransaction


class TransactionServer(override val executionContext:ServerExecutionContext,
                        override val authOpts: AuthOptions,
                        override val storageOpts: StorageOptions,
                        override val rocksStorageOpts: RocksStorageOptions)
  extends TransactionDataServiceImpl
    with TransactionMetaServiceImpl
    with ConsumerServiceImpl
    with StreamServiceImpl {

  override val consumerEnvironment: Environment = transactionMetaEnviroment

  override def putConsumerTransaction(consumerTransaction: ConsumerTransactionKey):Boolean = setConsumerState(consumerTransaction)
  override def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionKey]): Boolean = setConsumerStates(consumerTransactions)

  def shutdown() = {
    closeTransactionDataDatabases()
  }
}