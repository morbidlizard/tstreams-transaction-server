package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.StreamCache
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{KeyStream, StreamWithoutKey}
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionKey
import transactionService.rpc.TransactionStates._
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Transaction, TransactionStates}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

trait TransactionStateHandler extends StreamCache {

  private final def isThisProducerTransactionExpired(currentTxn: ProducerTransactionKey, nextTxn: ProducerTransactionKey): Boolean = {
    (currentTxn.timestamp + TimeUnit.SECONDS.toMillis(currentTxn.ttl)) <= nextTxn.timestamp
  }


  //  (ts.state, update.state) match {
  //    /*
  //    from opened to *
  //     */
  //    case (TransactionStatus.opened, TransactionStatus.opened) =>
  //
  //    case (TransactionStatus.opened, TransactionStatus.update) =>
  //      ts.queueOrderID = orderID
  //      ts.state = TransactionStatus.opened
  //      ts.ttl = System.currentTimeMillis() + update.ttl * 1000
  //
  //    case (TransactionStatus.opened, TransactionStatus.cancel) =>
  //      ts.state = TransactionStatus.invalid
  //      ts.ttl = 0L
  //      stateMap.remove(update.transactionID)
  //
  //    case (TransactionStatus.opened, TransactionStatus.`checkpointed`) =>
  //      ts.queueOrderID = orderID
  //      ts.state = TransactionStatus.checkpointed
  //      ts.itemCount = update.itemCount
  //      ts.ttl = Long.MaxValue
  //
  //    /*
  //    from update -> * no implement because opened
  //     */
  //    case (TransactionStatus.update, _) =>
  //
  //    /*
  //       */
  //    case (TransactionStatus.invalid, _) =>
  //
  //    /*
  //    from cancel -> * no implement because removed
  //    */
  //    case (TransactionStatus.cancel, _) =>
  //
  //    /*
  //    from post -> *
  //     */
  //    case (TransactionStatus.`checkpointed`, _) =>
  //  }

  private final def transiteProducerTransactiontoInvalidState(txn: ProducerTransactionKey) = {
    ProducerTransactionKey(
      Key(txn.stream, txn.partition, txn.transactionID),
      ProducerTransactionWithoutKey(Invalid, txn.quantity, 0L, txn.timestamp)
    )
  }

  @throws[IllegalArgumentException]
  private final def transiteProducerTransactiontoNewState(currentTxn: ProducerTransactionKey, nextTxn: ProducerTransactionKey): ProducerTransactionKey =
    (currentTxn.state, nextTxn.state) match {
      case (Opened, Opened) => currentTxn

      case (Opened, Updated) =>
        if (isThisProducerTransactionExpired(currentTxn, nextTxn)) transiteProducerTransactiontoInvalidState(currentTxn)
        else
          ProducerTransactionKey(
            Key(nextTxn.stream, nextTxn.partition, nextTxn.transactionID),
            ProducerTransactionWithoutKey(Opened, nextTxn.quantity, nextTxn.ttl, nextTxn.timestamp)
          )

      case (Opened, Cancel) => transiteProducerTransactiontoInvalidState(currentTxn)

      case (Opened, Invalid) => throw new IllegalArgumentException("An opened transaction can transite to the Invalid state by Cancel state only!")

      case (Opened, Checkpointed) =>
        if (isThisProducerTransactionExpired(currentTxn, nextTxn))
          transiteProducerTransactiontoInvalidState(currentTxn)
        else
          nextTxn

      case (Updated, _) => throw new IllegalArgumentException("A transaction with Updated state can't be a root of transactions chain.")
      case (Cancel, _) => throw new IllegalArgumentException("A transaction with Cancel state can't be a root of transactions chain.")

      case (Invalid, _) => currentTxn
      case (Checkpointed, _) => currentTxn

      case (_, _) => throw new IllegalArgumentException("Unknown States should be implemented")
    }


  @tailrec @throws[IllegalArgumentException]
  private final def process(txns: List[ProducerTransactionKey]): ProducerTransactionKey = txns match {
    case Nil => throw new IllegalArgumentException
    case head::Nil => head.state match {
      case Opened => head
      case state: TransactionStates => throw new IllegalArgumentException(s"A transaction with $state state can't be a root of transactions chain.")
    }
    case head::next::Nil  =>
      if ((head.state == Invalid) || (head.state == Checkpointed)) head
      else transiteProducerTransactiontoNewState(head, next)
    case head::next::tail =>
      if ((head.state == Invalid) || (head.state == Checkpointed)) head
      else process(transiteProducerTransactiontoNewState(head, next) :: tail)
  }

  final def decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions: Seq[(Transaction, Long)]) = {
    val producerTransactions = ArrayBuffer[(ProducerTransaction, Long)]()
    val consumerTransactions = ArrayBuffer[(ConsumerTransaction, Long)]()

    transactions foreach {case(transaction, timestamp) =>
      (transaction.producerTransaction, transaction.consumerTransaction) match {
        case (Some(txn), _) => producerTransactions += ((txn, timestamp))
        case (_, Some(txn)) => consumerTransactions += ((txn, timestamp))
        case  _ =>
      }
    }
    (producerTransactions, consumerTransactions)
  }

  final def groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(txns: Seq[(ProducerTransaction, Long)]): Map[KeyStream, Seq[ProducerTransactionKey]] = {
    txns.map { case (producerTransaction, timestamp) =>
      val keyStreams = getStreamFromOldestToNewest(producerTransaction.stream)
      val streamForThisTransaction = keyStreams.filter(_.stream.timestamp <= timestamp).last
      (streamForThisTransaction, ProducerTransactionKey(producerTransaction, streamForThisTransaction.streamNameToLong, timestamp))
    }.groupBy(txn => txn._1)
      .withFilter(x => !x._1.stream.deleted)
      .map { case (stream, txns) => (stream, txns.map(_._2)) }
  }


  //  @throws[StreamNotExist]
  final def decomposeConsumerTransactionsToDatabaseRepresentation(transactions: Seq[(ConsumerTransaction, Long)]) = {
    val consumerTransactionsKey = ArrayBuffer[ConsumerTransactionKey]()
    transactions foreach { case (txn, timestamp) => scala.util.Try {
      val streamForThisTransaction = getStreamFromOldestToNewest(txn.stream).filter(_.stream.timestamp <= timestamp).last
      consumerTransactionsKey += ConsumerTransactionKey(txn, streamForThisTransaction.streamNameToLong, timestamp)
    }}

    consumerTransactionsKey
  }

  //  @throws[StreamNotExist]
  //  final def decomposeTransactions(transactions: Seq[(Transaction, Long)]): (Seq[ProducerTransactionKey], Seq[ConsumerTransactionKey]) = {
  //    val producerTransactions = ArrayBuffer[ProducerTransactionKey]()
  //    val consumerTransactions = ArrayBuffer[ConsumerTransactionKey]()
  //
  //    transactions foreach {case(transaction, timestamp) =>
  //      (transaction.producerTransaction, transaction.consumerTransaction) match {
  //        case (Some(txn), _) => scala.util.Try {
  //          val streamDatabase = getStreamDatabaseObject(txn.stream)
  //          val newTxn = if (txn.state == Checkpointed)
  //            ProducerTransactionKey(ProducerTransaction(txn.stream, txn.partition, txn.transactionID, txn.state, txn.quantity, streamDatabase.ttl), streamDatabase.streamNameToInt, timestamp)
  //          else
  //            ProducerTransactionKey(txn, streamDatabase.streamNameToInt, timestamp)
  //
  //          producerTransactions += newTxn
  //        }
  //        case (_, Some(txn)) => scala.util.Try {
  //          val streamNameToInt = getStreamDatabaseObject(txn.stream).streamNameToInt
  //          consumerTransactions += ConsumerTransactionKey(txn, streamNameToInt, timestamp)
  //        }
  //        case  _ =>
  //      }
  //    }
  //    (producerTransactions, consumerTransactions)
  //  }

  final def groupProducerTransactions(producerTransactions: Seq[ProducerTransactionKey]) = producerTransactions.groupBy(txn => txn.key)


  @throws[IllegalArgumentException]
  final def transiteProducerTransactionToNewState(dbTransaction: ProducerTransactionKey, commitLogTransactions: Seq[ProducerTransactionKey]): ProducerTransactionKey = {
    process(dbTransaction :: commitLogTransactions.sortBy(_.timestamp).toList)
  }

  @throws[IllegalArgumentException]
  final def transiteProducerTransactionToNewState(commitLogTransactions: Seq[ProducerTransactionKey]): ProducerTransactionKey = {
    process(commitLogTransactions.sortBy(_.timestamp).toList)
  }
}
