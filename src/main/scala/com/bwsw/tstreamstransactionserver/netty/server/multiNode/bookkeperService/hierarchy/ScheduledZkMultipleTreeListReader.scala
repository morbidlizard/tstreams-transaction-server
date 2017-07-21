package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerTransactionKey, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerIDAndItsLastRecordID, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.rpc.Transaction

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class ScheduledZkMultipleTreeListReader(zkMultipleTreeListReader: ZkMultipleTreeListReader,
                                        rocksReader: RocksReader,
                                        rocksWriter: RocksWriter)
  extends Runnable
{

  private def getBigCommit(processedLastRecordIDsAcrossLedgers: Array[LedgerIDAndItsLastRecordID]): BigCommit = {
    val value = MetadataRecord(processedLastRecordIDsAcrossLedgers).toByteArray
    new BigCommit(rocksWriter, RocksStorage.BOOKKEEPER_LOG_STORE, BigCommit.bookkeeperKey, value)
  }


  private def putConsumerTransaction(consumerRecords: java.util.Map[ConsumerTransactionKey, ConsumerTransactionRecord],
                                     consumerTransactionRecord: ConsumerTransactionRecord): Unit = {
    Option(
      consumerRecords.computeIfPresent(
        consumerTransactionRecord.key,
        (_, oldConsumerTransactionRecord) => {
          if (consumerTransactionRecord.timestamp < oldConsumerTransactionRecord.timestamp)
            oldConsumerTransactionRecord
          else
            consumerTransactionRecord
        }
      )
    ).getOrElse(
      consumerRecords.put(
        consumerTransactionRecord.key,
        consumerTransactionRecord)
    )
  }

  private def putProducerTransaction(producerRecords: ArrayBuffer[ProducerTransactionRecord],
                                     producerTransactionRecord: ProducerTransactionRecord) = {
    producerRecords += producerTransactionRecord
  }

  private def decomposeTransaction(producerRecords: ArrayBuffer[ProducerTransactionRecord],
                                   consumerRecords: java.util.Map[ConsumerTransactionKey, ConsumerTransactionRecord],
                                   transaction: Transaction,
                                   timestamp: Long) = {
    transaction.consumerTransaction.foreach { consumerTransaction =>
      val consumerTransactionRecord =
        ConsumerTransactionRecord(consumerTransaction, timestamp)
      putConsumerTransaction(consumerRecords, consumerTransactionRecord)
    }

    transaction.producerTransaction.foreach { producerTransaction =>
      val producerTransactionRecord =
        ProducerTransactionRecord(producerTransaction, timestamp)
      putProducerTransaction(producerRecords, producerTransactionRecord)
    }
  }

  def processAndPersistRecords(): PersistedCommitAndMoveToNextRecordsInfo = {
    val ledgerRecordIDs = rocksReader
      .getLastProcessedLedgersAndRecordIDs
      .getOrElse(Array.empty[LedgerIDAndItsLastRecordID])


    val (records, ledgerIDsAndTheirLastRecordIDs) =
      zkMultipleTreeListReader.process(ledgerRecordIDs)

    if (records.isEmpty) {
      PersistedCommitAndMoveToNextRecordsInfo(
        isCommitted = true,
        doReadNextRecords = false
      )
    }
    else
    {
      val bigCommit = getBigCommit(ledgerIDsAndTheirLastRecordIDs)

      val recordsByType = records.groupBy(record => record.recordType)

      val producerRecords = new ArrayBuffer[ProducerTransactionRecord]()
      val consumerRecords = new java.util.HashMap[ConsumerTransactionKey, ConsumerTransactionRecord]()


      recordsByType.get(RecordType.PutTransactionDataType)
        .foreach(records =>
          records.foreach { record =>
            val producerData =
              RecordType.deserializePutTransactionData(record.body)

            bigCommit.putProducerData(
              producerData.streamID,
              producerData.partition,
              producerData.transaction,
              producerData.data,
              producerData.from
            )
          })

      recordsByType.get(RecordType.PutProducerStateWithDataType)
        .foreach(records =>
          records.foreach { record =>
            val producerTransactionAndData =
              RecordType.deserializePutProducerStateWithData(record.body)

            val producerTransactionRecord =
              ProducerTransactionRecord(
                producerTransactionAndData.transaction,
                record.timestamp
              )

            bigCommit.putProducerData(
              producerTransactionRecord.stream,
              producerTransactionRecord.partition,
              producerTransactionRecord.transactionID,
              producerTransactionAndData.data,
              producerTransactionAndData.from
            )

            putProducerTransaction(producerRecords, producerTransactionRecord)
          })

      recordsByType.get(RecordType.PutConsumerCheckpointType)
        .foreach(records =>
          records.foreach { record =>
            val consumerTransactionArgs = RecordType.deserializePutConsumerCheckpoint(record.body)
            val consumerTransactionRecord = {
              import consumerTransactionArgs._
              ConsumerTransactionRecord(name,
                streamID,
                partition,
                transaction,
                record.timestamp
              )
            }
            putConsumerTransaction(consumerRecords, consumerTransactionRecord)
          })


      recordsByType.get(RecordType.PutTransactionType)
        .foreach { records =>
          records.foreach { record =>
            val transaction = RecordType.deserializePutTransaction(record.body)
              .transaction
            decomposeTransaction(producerRecords, consumerRecords, transaction, record.timestamp)
          }
        }

      recordsByType.get(RecordType.PutTransactionsType)
        .foreach(records =>
          records.foreach { record =>
            val transactions = RecordType.deserializePutTransactions(record.body)
              .transactions

            transactions.foreach(transaction =>
              decomposeTransaction(producerRecords, consumerRecords, transaction, record.timestamp)
            )
          })

      bigCommit.putProducerTransactions(
        producerRecords.sorted
      )

      bigCommit.putConsumerTransactions(
        consumerRecords.values().asScala.toSeq
      )
      PersistedCommitAndMoveToNextRecordsInfo(
        isCommitted = bigCommit.commit(),
        doReadNextRecords = true
      )
    }
  }

  override def run(): Unit = {
    var doReadNextRecords = true
    while (doReadNextRecords) {
      val info = processAndPersistRecords()
      doReadNextRecords = info.doReadNextRecords
    }
  }
}
