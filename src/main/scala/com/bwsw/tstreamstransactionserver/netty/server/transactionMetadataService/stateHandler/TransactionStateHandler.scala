package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.StreamCache
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionKey
import com.bwsw.tstreamstransactionserver.netty.server.streamService.KeyStream
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionWithoutKey}
import transactionService.rpc.TransactionStates._
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Transaction, TransactionStates}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

trait TransactionStateHandler extends LastTransactionStreamPartition with StreamCache {
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
      com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.Key(txn.stream, txn.partition, txn.transactionID),
      ProducerTransactionWithoutKey(Invalid, txn.quantity, 0L, txn.timestamp)
    )
  }
  private final def isThisProducerTransactionExpired(currentTxn: ProducerTransactionKey, nextTxn: ProducerTransactionKey): Boolean = {
    (currentTxn.timestamp + TimeUnit.SECONDS.toMillis(currentTxn.ttl)) <= nextTxn.timestamp
  }

  @throws[IllegalArgumentException]
  private final def transiteProducerTransactionToNewState(currentTxn: ProducerTransactionKey, nextTxn: ProducerTransactionKey): ProducerTransactionKey =
    (currentTxn.state, nextTxn.state) match {
      case (Opened, Opened) => currentTxn

      case (Opened, Updated) =>
        if (isThisProducerTransactionExpired(currentTxn, nextTxn)) transiteProducerTransactiontoInvalidState(currentTxn)
        else
          ProducerTransactionKey(
            com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.Key(nextTxn.stream, nextTxn.partition, nextTxn.transactionID),
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
      else transiteProducerTransactionToNewState(head, next)
    case head::next::tail =>
      if ((head.state == Invalid) || (head.state == Checkpointed)) head
      else process(transiteProducerTransactionToNewState(head, next) :: tail)
  }


  private type Timestamp = Long
  final def decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions: Seq[(transactionService.rpc.Transaction, Timestamp)]) = {
    val producerTransactions = ArrayBuffer[(ProducerTransaction, Timestamp)]()
    val consumerTransactions = ArrayBuffer[(ConsumerTransaction, Timestamp)]()

    transactions foreach {case(transaction, timestamp) =>
      (transaction.producerTransaction, transaction.consumerTransaction) match {
        case (Some(txn), _) =>
          val stream = getStreamFromOldestToNewest(txn.stream).last
          val key = KeyStreamPartition(stream.streamNameToLong, txn.partition)
          if (!isThatTransactionOutOfOrder(key, txn.transactionID)) {
            updateLastTransactionStreamPartitionRamTable(key, txn.transactionID)
            producerTransactions += ((txn, timestamp))
          }

        case (_, Some(txn)) =>
          val stream = getStreamFromOldestToNewest(txn.stream).last
          val key = KeyStreamPartition(stream.streamNameToLong, txn.partition)
          if (!isThatTransactionOutOfOrder(key, txn.transactionID))
            consumerTransactions += ((txn, timestamp))

        case  _ =>
      }
    }
    (producerTransactions, consumerTransactions)
  }

  final def groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(txns: Seq[(ProducerTransaction, Timestamp)], berkeleyTransaction: com.sleepycat.je.Transaction): Map[KeyStream, ArrayBuffer[ProducerTransactionKey]] =
    txns.foldLeft[scala.collection.mutable.Map[KeyStream, ArrayBuffer[ProducerTransactionKey]]](scala.collection.mutable.Map()) { case (acc, (producerTransaction, timestamp)) =>
      val keyStreams = getStreamFromOldestToNewest(producerTransaction.stream)
      val streamForThisTransaction = keyStreams.filter(_.stream.timestamp <= timestamp).lastOption
      streamForThisTransaction match {
        case Some(keyStream) if !keyStream.stream.deleted =>
          val key = KeyStreamPartition(keyStream.streamNameToLong, producerTransaction.partition)
          putLastTransaction(key, producerTransaction.transactionID, berkeleyTransaction)

          if (acc.contains(keyStream))
            acc(keyStream) += ProducerTransactionKey(producerTransaction, keyStream.streamNameToLong, timestamp)
          else
            acc += ((keyStream, ArrayBuffer(ProducerTransactionKey(producerTransaction, keyStream.streamNameToLong, timestamp))))

          acc
        case _ => acc
      }
    }.toMap


  //  @throws[StreamNotExist]
  final def decomposeConsumerTransactionsToDatabaseRepresentation(transactions: Seq[(ConsumerTransaction, Timestamp)]) = {
    val consumerTransactionsKey = ArrayBuffer[ConsumerTransactionKey]()
    transactions foreach { case (txn, timestamp) => scala.util.Try {
      val streamForThisTransaction = getStreamFromOldestToNewest(txn.stream).filter(_.stream.timestamp <= timestamp).last
      consumerTransactionsKey += ConsumerTransactionKey(txn, streamForThisTransaction.streamNameToLong, timestamp)
    }}

    consumerTransactionsKey
  }

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
