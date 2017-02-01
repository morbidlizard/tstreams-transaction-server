package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.configProperties.ServerConfig
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetaService.TransactionMetaServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.сonsumerService.ConsumerServiceImpl
import com.sleepycat.je.{Environment, Transaction}
import transactionService.rpc.ConsumerTransaction


class TransactionServer(override val config: ServerConfig)
  extends TransactionDataServiceImpl
    with TransactionMetaServiceImpl
    with ConsumerServiceImpl
    with StreamServiceImpl {
  override val consumerEnvironment: Environment = transactionMetaEnviroment

  override def putConsumerTransaction(databaseTxn: Transaction, txn: ConsumerTransaction): Boolean = {
    setConsumerState(databaseTxn, txn.name, txn.stream, txn.partition, txn.transactionID)
  }

  def shutdown() = {
    closeTransactionDataDatabases()
  }
}