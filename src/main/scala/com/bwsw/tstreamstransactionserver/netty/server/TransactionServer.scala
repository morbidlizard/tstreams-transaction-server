package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceImpl, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.sleepycat.je.Transaction

import scala.concurrent.{Future => ScalaFuture}



class TransactionServer(override val executionContext: ServerExecutionContext,
                        override val authOpts: AuthOptions,
                        override val storageOpts: StorageOptions,
                        override val rocksStorageOpts: RocksStorageOptions,
                        override val timer: Time = new Time{}
                       )
  extends HasEnvironment with StreamServiceImpl with TransactionMetaServiceImpl with ConsumerServiceImpl with TransactionDataServiceImpl
{
  override def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord], parentBerkeleyTxn: com.sleepycat.je.Transaction): Unit = putConsumersCheckpoints(consumerTransactions, parentBerkeleyTxn)
  override def closeRocksDBConnectionAndDeleteFolder(stream: Long): Unit = removeRocksDBDatabaseAndDeleteFolder(stream)
  override def removeLastOpenedAndCheckpointedTransactionRecords(stream: Long, transaction: Transaction): Unit = deleteLastOpenedAndCheckpointedTransactions(stream, transaction)

  final def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases(): Unit = {
    stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
    closeAllDatabases()
  }

  final def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    executionContext.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
  }

  final def closeAllDatabases(): Unit = {
    closeStreamDatabase()
    closeLastTransactionStreamPartitionDatabases()
    closeTransactionDataDatabases()
    closeConsumerDatabase()
    closeTransactionMetaDatabases()

    closeTransactionMetaEnvironment()
  }
}