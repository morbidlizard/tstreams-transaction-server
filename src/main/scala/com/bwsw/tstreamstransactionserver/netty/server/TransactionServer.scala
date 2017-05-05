package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceImpl, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.Batch
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl
import com.bwsw.tstreamstransactionserver.options.ServerOptions._

import scala.collection.mutable.ListBuffer


class TransactionServer(override val executionContext: ServerExecutionContext,
                        override val authOpts: AuthOptions,
                        override val storageOpts: StorageOptions,
                        override val rocksStorageOpts: RocksStorageOptions,
                        override val timer: Time = new Time{}
                       )
  extends RocksStorage with StreamServiceImpl with TransactionMetaServiceImpl with ConsumerServiceImpl with TransactionDataServiceImpl
{
  override def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord], batch: Batch): ListBuffer[Unit => Unit] = putConsumersCheckpoints(consumerTransactions, batch)
  override def closeRocksDBConnectionAndDeleteFolder(stream: Long): Unit = removeRocksDBDatabaseAndDeleteFolder(stream)
  override def removeLastOpenedAndCheckpointedTransactionRecords(stream: Long, batch: Batch): Unit = deleteLastOpenedAndCheckpointedTransactions(stream, batch)

  final def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases(): Unit = {
    stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
    closeAllDatabases()
  }

  final def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    executionContext.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
  }

  final def closeAllDatabases(): Unit = {
    rocksMetaServiceDB.close()
    closeTransactionDataDatabases()
  }
}