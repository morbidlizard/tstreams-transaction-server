package com.bwsw.tstreamstransactionserver.netty.server
import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{CommitLogKey, ProducerTransactionRecord}
import org.slf4j.{Logger, LoggerFactory}

object BigCommit {
  val bookkeeperKey: Array[Byte] = "key".getBytes()
  val commitLogKey: Array[Byte] = "file".getBytes()
}


class BigCommit(rocksWriter: RocksWriter,
                databaseIndex: Int,
                key: Array[Byte],
                value: Array[Byte]) {

  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private val batch =
    rocksWriter.getNewBatch

  private lazy val notifications =
    new scala.collection.mutable.ListBuffer[Unit => Unit]


  def putProducerTransactions(producerTransactions: Seq[ProducerTransactionRecord]): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(s"[batch] " +
        s"Adding producer transactions to commit.")
    }

    notifications ++=
      rocksWriter.putTransactions(
        producerTransactions,
        batch
      )
  }

  def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord]): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(s"[batch] " +
        s"Adding consumer transactions to commit.")
    }

    notifications ++=
      rocksWriter.putConsumersCheckpoints(
        consumerTransactions,
        batch
      )
  }

  def putProducerData(streamID: Int,
                      partition: Int,
                      transaction: Long,
                      data: Seq[ByteBuffer],
                      from: Int): Boolean = {
    val isDataPersisted = rocksWriter.putTransactionData(
      streamID,
      partition,
      transaction,
      data,
      from
    )

    if (logger.isDebugEnabled) {
      logger.debug(s"[batch] " +
        s"Persisting producer transactions data on stream: $streamID, partition: $partition, transaction: $transaction, " +
        s"from: $from.")
    }

    isDataPersisted
  }

  def commit(): Boolean = {
    batch.put(databaseIndex, key, value)
    if (batch.write()) {
      if (logger.isDebugEnabled) logger.debug(s"commit is successfully fixed.")
      notifications foreach (notification => notification(()))
      true
    } else {
      false
    }
  }
}
