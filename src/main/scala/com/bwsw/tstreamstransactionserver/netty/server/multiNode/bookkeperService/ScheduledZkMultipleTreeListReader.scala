package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerTransactionKey, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.{RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerIDAndItsLastRecordID
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.rpc.Transaction

import scala.collection.mutable

class ScheduledZkMultipleTreeListReader(zkMultipleTreeListReader: ZkMultipleTreeListReader,
                                        transactionServer: TransactionServer) {
  private type Timestamp = Long

  val (records, ledgerIDsAndTheirLastRecordIDs) =
    zkMultipleTreeListReader.process(Array.empty[LedgerIDAndItsLastRecordID])



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

  private def decomposeTransaction(producerRecords: mutable.PriorityQueue[ProducerTransactionRecord],
                                   consumerRecords: java.util.Map[ConsumerTransactionKey, ConsumerTransactionRecord],
                                   transaction: Transaction,
                                   timestamp: Long) = {
    transaction.consumerTransaction.foreach { consumerTransaction =>
      val consumerTransactionRecord =
        ConsumerTransactionRecord(consumerTransaction, timestamp)
      putConsumerTransaction(consumerRecords, consumerTransactionRecord)
    }

    transaction.producerTransaction.foreach { producerTransaction =>
      producerRecords += ProducerTransactionRecord(producerTransaction, timestamp)
    }
  }

  def test(array: Array[Record]) = {
    val recordsByType = array.groupBy(record => record.recordType)

    val producerRecords = new mutable.PriorityQueue[ProducerTransactionRecord]()
    val consumerRecords = new java.util.HashMap[ConsumerTransactionKey, ConsumerTransactionRecord]()


    recordsByType.get(RecordType.PutTransactionType)
      .foreach(records =>
        records.foreach { record =>
          val transaction = RecordType.deserializePutTransaction(record.body)
            .transaction

          decomposeTransaction(producerRecords, consumerRecords, transaction, record.timestamp)

        })

    recordsByType.get(RecordType.PutTransactionsType)
      .foreach(records =>
        records.foreach { record =>
          val transactions = RecordType.deserializePutTransactions(record.body)
            .transactions

          transactions.foreach(transaction =>
            decomposeTransaction(producerRecords, consumerRecords, transaction, record.timestamp)
          )
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

//    recordsByType.get(RecordType.PutProducerStateWithDataType)
//      .foreach(records =>
//        records.foreach { record =>
//          val producerTransactionAndData =
//            RecordType.deserializePutProducerStateWithData(record.body)
//
//          producerTransactionAndData.
//
//        })
  }
}
