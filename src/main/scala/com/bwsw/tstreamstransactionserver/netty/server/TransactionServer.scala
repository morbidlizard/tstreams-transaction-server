package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionKey
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceImpl
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import com.sleepycat.je.{Durability, Environment, EnvironmentConfig}



class TransactionServer(override val executionContext: ServerExecutionContext,
                        override val authOpts: AuthOptions,
                        override val storageOpts: StorageOptions,
                        override val rocksStorageOpts: RocksStorageOptions
                       )
  extends HaveEnvironment with StreamServiceImpl with TransactionMetaServiceImpl with ConsumerServiceImpl with TransactionDataServiceImpl
{
  override def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionKey], parentBerkeleyTxn: com.sleepycat.je.Transaction): Unit = setConsumerStates(consumerTransactions, parentBerkeleyTxn)

  def shutdown() = {
    executionContext.shutdown()

    closeStreamEnvironmentAndDatabase()

    closeConsumerDatabase()

    closeTransactionMetaDatabases()
    closeTransactionMetaEnvironment()

    closeTransactionDataDatabases()
  }
}