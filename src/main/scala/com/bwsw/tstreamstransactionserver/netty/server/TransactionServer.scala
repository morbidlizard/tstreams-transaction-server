package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetadataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.сonsumerService.ConsumerServiceImpl
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.sleepycat.je.{Environment, Transaction}
import transactionService.rpc.ConsumerTransaction


class TransactionServer(override val executionContext:ServerExecutionContext,
                        override val authOpts: AuthOptions,
                        override val storageOpts: StorageOptions,
                        override val rocksStorageOpts: RocksStorageOptions)
  extends TransactionDataServiceImpl
    with TransactionMetadataServiceImpl
    with ConsumerServiceImpl
    with StreamServiceImpl {

  override val consumerEnvironment: Environment = transactionMetaEnviroment

  override def putConsumerTransaction(databaseTxn: Transaction, txn: ConsumerTransaction): Boolean = {
    setConsumerState(databaseTxn, txn.name, txn.stream, txn.partition, txn.transactionID)
  }

  def shutdown() = {
    executionContext.shutdown()

    closeStreamEnvironmentAndDatabase()

    closeConsumerDatabase()
    closeTransactionMetaDatabases()
    closeTransactionMetaEnvironment()

    closeTransactionDataDatabases()
  }
}