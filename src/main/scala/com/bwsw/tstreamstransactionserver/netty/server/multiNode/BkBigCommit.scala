package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerIDAndItsLastRecordID, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.{Commitable, RocksStorage, TransactionServer}
import org.slf4j.{Logger, LoggerFactory}

private object BkBigCommit {
  val databaseKey: Array[Byte] = {
    val size = java.lang.Integer.BYTES
    val buffer = java.nio.ByteBuffer
      .allocate(size)
      .putLong(-1L)
    buffer.flip()

    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    bytes
  }
}

class BkBigCommit(transactionServer: TransactionServer,
                  processedLastRecordIDsAcrossLedgers: Array[LedgerIDAndItsLastRecordID])
  extends Commitable {

  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private val batch =
    transactionServer.getNewBatch

  private lazy val notifications =
    new scala.collection.mutable.ListBuffer[Unit => Unit]


  override def putProducerTransactions(producerTransactions: Seq[ProducerTransactionRecord]): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(s"[batch ${batch.id}] " +
        s"Adding producer transactions to commit.")
    }

    notifications ++=
      transactionServer.putTransactions(
        producerTransactions,
        batch
      )
  }

  override def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord]): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(s"[batch ${batch.id}] " +
        s"Adding consumer transactions to commit.")
    }

    notifications ++=
      transactionServer.putConsumersCheckpoints(
        consumerTransactions,
        batch
      )
  }

  override def putProducerData(streamID: Int,
                               partition: Int,
                               transaction: Long,
                               data: Seq[ByteBuffer],
                               from: Int): Boolean = {
    val isDataPersisted = transactionServer.putTransactionData(
      streamID,
      partition,
      transaction,
      data,
      from
    )

    if (logger.isDebugEnabled) {
      logger.debug(s"[batch ${batch.id}] " +
        s"Persisting producer transactions data on stream: $streamID, partition: $partition, transaction: $transaction, " +
        s"from: $from.")
    }

    isDataPersisted
  }

  override def commit(): Boolean = {
    val key   = BkBigCommit.databaseKey
    val value = MetadataRecord(processedLastRecordIDsAcrossLedgers)
      .toByteArray

    batch.put(RocksStorage.COMMIT_LOG_STORE, key, value)
    if (batch.write()) {
      if (logger.isDebugEnabled) logger.debug(s"commit ${batch.id} is successfully fixed.")
      notifications foreach (notification => notification(()))
      true
    } else {
      false
    }
  }
}
