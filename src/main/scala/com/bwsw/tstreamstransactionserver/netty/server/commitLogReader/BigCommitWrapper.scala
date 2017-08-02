package com.bwsw.tstreamstransactionserver.netty.server.commitLogReader

import com.bwsw.tstreamstransactionserver.netty.server.BigCommit
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerTransactionKey, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.rpc.Transaction

import scala.collection.mutable

class BigCommitWrapper(bigCommit: BigCommit) {

  private val producerRecords =
    mutable.ArrayBuffer.empty[ProducerTransactionRecord]
  private val consumerRecords =
    mutable.Map.empty[ConsumerTransactionKey, ConsumerTransactionRecord]

  private def putConsumerTransaction(consumerRecords: mutable.Map[ConsumerTransactionKey, ConsumerTransactionRecord],
                                     consumerTransactionRecord: ConsumerTransactionRecord): Unit = {
    val txnForUpdateOpt = consumerRecords
      .get(consumerTransactionRecord.key)

    txnForUpdateOpt match {
      case Some(oldTxn)
        if consumerTransactionRecord.timestamp > oldTxn.timestamp =>
        consumerRecords.put(
          consumerTransactionRecord.key,
          consumerTransactionRecord
        )
      case None =>
        consumerRecords.put(
          consumerTransactionRecord.key,
          consumerTransactionRecord
        )
      case _ =>
    }
  }

  private def putProducerTransaction(producerRecords: mutable.ArrayBuffer[ProducerTransactionRecord],
                                     producerTransactionRecord: ProducerTransactionRecord) = {
    producerRecords += producerTransactionRecord
  }

  private def decomposeTransaction(producerRecords: mutable.ArrayBuffer[ProducerTransactionRecord],
                                   consumerRecords: mutable.Map[ConsumerTransactionKey, ConsumerTransactionRecord],
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

  def commit(): Boolean = {
    bigCommit.putProducerTransactions(
      producerRecords.sorted
    )

    bigCommit.putConsumerTransactions(
      consumerRecords.values.toIndexedSeq
    )

    producerRecords.clear()
    consumerRecords.clear()

    bigCommit.commit()
  }

  def addFrames(frames: Seq[Frame]): Unit = {
    val recordsByType = frames.groupBy(frame => Frame(frame.typeId))

    recordsByType.get(Frame.PutTransactionDataType)
      .foreach(records =>
        records.foreach { record =>
          val producerData =
            Frame.deserializePutTransactionData(record.body)

          bigCommit.putProducerData(
            producerData.streamID,
            producerData.partition,
            producerData.transaction,
            producerData.data,
            producerData.from
          )
        })

    recordsByType.get(Frame.PutProducerStateWithDataType)
      .foreach(records =>
        records.foreach { record =>
          val producerTransactionAndData =
            Frame.deserializePutProducerStateWithData(record.body)

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

    recordsByType.get(Frame.PutConsumerCheckpointType)
      .foreach(records =>
        records.foreach { record =>
          val consumerTransactionArgs = Frame.deserializePutConsumerCheckpoint(record.body)
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


    recordsByType.get(Frame.PutTransactionType)
      .foreach { records =>
        records.foreach { record =>
          val transaction = Frame.deserializePutTransaction(record.body)
            .transaction
          decomposeTransaction(producerRecords, consumerRecords, transaction, record.timestamp)
        }
      }

    recordsByType.get(Frame.PutTransactionsType)
      .foreach(records =>
        records.foreach { record =>
          val transactions = Frame.deserializePutTransactions(record.body)
            .transactions

          transactions.foreach(transaction =>
            decomposeTransaction(producerRecords, consumerRecords, transaction, record.timestamp)
          )
        })
  }
}
