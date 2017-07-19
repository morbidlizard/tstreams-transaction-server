package com.bwsw.tstreamstransactionserver.netty.server

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerIDAndItsLastRecordID
import com.bwsw.tstreamstransactionserver.netty.server.storage.AllInOneRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService._
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{LastOpenedAndCheckpointedTransaction, LastTransactionStreamPartition}
import com.bwsw.tstreamstransactionserver.rpc._

import scala.collection.Set

class RocksReader(rocksStorage: AllInOneRockStorage,
                  lastTransactionStreamPartition: LastTransactionStreamPartition,
                  transactionDataService: TransactionDataServiceImpl) {

  private val consumerServiceImpl = new ConsumerServiceImpl(
    rocksStorage.getRocksStorage
  )

  private val transactionIDService =
    com.bwsw.tstreamstransactionserver.netty.server.transactionIDService.TransactionIdService

  private val transactionMetaServiceImpl = new TransactionMetaServiceImpl(
    rocksStorage.getRocksStorage,
    lastTransactionStreamPartition,
    consumerServiceImpl
  )

  final def getLastProcessedCommitLogFileID: Long =
    transactionMetaServiceImpl.getLastProcessedCommitLogFileID.getOrElse(-1L)

  final def getLastProcessedLedgersAndRecordIDs: Option[Array[LedgerIDAndItsLastRecordID]] =
    transactionMetaServiceImpl.getLastProcessedLedgerAndRecordIDs

  final def getTransactionID: Long =
    transactionIDService.getTransaction()

  final def getTransactionIDByTimestamp(timestamp: Long): Long =
    transactionIDService.getTransaction(timestamp)

  final def getTransaction(streamID: Int, partition: Int, transaction: Long): TransactionInfo =
    transactionMetaServiceImpl.getTransaction(streamID, partition, transaction)

  final def getOpenedTransaction(key: ProducerTransactionKey): Option[ProducerTransactionValue] =
    transactionMetaServiceImpl.getOpenedTransaction(key)

  final def getLastCheckpointedTransaction(streamID: Int, partition: Int): Option[Long] =
    lastTransactionStreamPartition.getLastTransactionIDAndCheckpointedID(streamID, partition)
      .flatMap(_.checkpointed.map(txn => txn.id)).orElse(Some(-1L))

  final def getLastTransactionIDAndCheckpointedID(streamID: Int, partition: Int): Option[LastOpenedAndCheckpointedTransaction] =
    lastTransactionStreamPartition.getLastTransactionIDAndCheckpointedID(streamID, partition)

  final def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: Set[TransactionStates]): ScanTransactionsInfo =
    transactionMetaServiceImpl.scanTransactions(streamID, partition, from, to, count, states)

  final def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int): Seq[ByteBuffer] = {
    transactionDataService.getTransactionData(streamID, partition, transaction, from, to)
  }

  final def getConsumerState(name: String, streamID: Int, partition: Int): Long = {
    consumerServiceImpl.getConsumerState(name, streamID, partition)
  }
}
