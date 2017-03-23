package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionKey
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceImpl
import com.bwsw.tstreamstransactionserver.options.ServerOptions._



class TransactionServer(override val executionContext: ServerExecutionContext,
                        override val authOpts: AuthOptions,
                        override val storageOpts: StorageOptions,
                        override val rocksStorageOpts: RocksStorageOptions,
                        override val timer: HasTime = new HasTime{}
                       )
  extends HasEnvironment with StreamServiceImpl with TransactionMetaServiceImpl with ConsumerServiceImpl with TransactionDataServiceImpl
{
  override def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionKey], parentBerkeleyTxn: com.sleepycat.je.Transaction): Unit = putConsumersCheckpoints(consumerTransactions, parentBerkeleyTxn)

  def shutdown() = {
    executionContext.shutdown()

    closeStreamDatabase()
    closeLastTransactionStreamPartitionDatabase()
    closeTransactionDataDatabases()
    closeConsumerDatabase()
    closeTransactionMetaDatabases()
    
    closeTransactionMetaEnvironment()

  }
}