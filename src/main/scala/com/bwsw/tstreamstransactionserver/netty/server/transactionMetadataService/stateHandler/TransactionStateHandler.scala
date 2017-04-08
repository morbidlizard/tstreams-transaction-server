package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionRecord, ProducerTransactionValue}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates._

import scala.annotation.tailrec

trait TransactionStateHandler {
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

  private[transactionMetadataService] def transitProducerTransactionToInvalidState(txn: ProducerTransactionRecord) = {
    ProducerTransactionRecord(
      com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionKey(txn.stream, txn.partition, txn.transactionID),
      ProducerTransactionValue(Invalid, 0, 0L, txn.timestamp)
    )
  }

  private def isThisProducerTransactionExpired(currentTxn: ProducerTransactionRecord, nextTxn: ProducerTransactionRecord): Boolean = {
    scala.math.abs(currentTxn.timestamp + TimeUnit.SECONDS.toMillis(currentTxn.ttl)) <= nextTxn.timestamp
  }

  @throws[IllegalArgumentException]
  private def transitProducerTransactionToNewState(currentTxn: ProducerTransactionRecord, nextTxn: ProducerTransactionRecord): ProducerTransactionRecord = {
    (currentTxn.state, nextTxn.state) match {
      case (Opened, Opened) => currentTxn

      case (Opened, Updated) =>
        if (isThisProducerTransactionExpired(currentTxn, nextTxn)) transitProducerTransactionToInvalidState(currentTxn)
        else
          ProducerTransactionRecord(
            com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionKey(nextTxn.stream, nextTxn.partition, nextTxn.transactionID),
            ProducerTransactionValue(Opened, nextTxn.quantity, nextTxn.ttl, nextTxn.timestamp)
          )

      case (Opened, Cancel) =>
        transitProducerTransactionToInvalidState(currentTxn)


      case (Opened, Invalid) => throw new IllegalArgumentException("An opened transaction can transit to the Invalid state by Cancel state only!")

      case (Opened, Checkpointed) =>
        if (isThisProducerTransactionExpired(currentTxn, nextTxn))
          transitProducerTransactionToInvalidState(currentTxn)
        else
          nextTxn

      case (Updated, _) => throw new IllegalArgumentException("A transaction with Updated state can't be a root of transactions chain.")
      case (Cancel, _) => throw new IllegalArgumentException("A transaction with Cancel state can't be a root of transactions chain.")

      case (Invalid, _) => currentTxn
      case (Checkpointed, _) => currentTxn

      case (thisState, thatState) => throw new IllegalArgumentException(s"Transition from $thisState to $thatState should be implemented.(Unknown states)")
    }
  }

  @tailrec
  @throws[IllegalArgumentException]
  private def process(txns: List[ProducerTransactionRecord]): ProducerTransactionRecord = txns match {
    case Nil => throw new IllegalArgumentException
    case head :: Nil => head.state match {
      case Opened => head
      case state: TransactionStates => throw new IllegalArgumentException(s"A transaction with $state state can't be a root of transactions chain.")
    }
    case head :: next :: Nil =>
      if ((head.state == Invalid) || (head.state == Checkpointed)) head
      else transitProducerTransactionToNewState(head, next)
    case head :: next :: tail =>
      if ((head.state == Invalid) || (head.state == Checkpointed)) head
      else process(transitProducerTransactionToNewState(head, next) :: tail)
  }

  @throws[IllegalArgumentException]
  private def processFirstTransaction(transaction: ProducerTransactionRecord) = {
    process(transaction::Nil)
  }

  @throws[IllegalArgumentException]
  final def transitProducerTransactionToNewState(transactionPersistedInBerkeleyDB: ProducerTransactionRecord, commitLogTransactions: Seq[ProducerTransactionRecord]): ProducerTransactionRecord = {
    val firstTransaction = processFirstTransaction(transactionPersistedInBerkeleyDB)
    process(firstTransaction :: commitLogTransactions.sortBy(_.transactionID).sortBy(_.timestamp).toList)
  }

  @throws[IllegalArgumentException]
  final def transitProducerTransactionToNewState(commitLogTransactions: Seq[ProducerTransactionRecord]): ProducerTransactionRecord = {
    if (commitLogTransactions.length > 1) {
      val orderedCommitLogTransactionsByTimestamp = commitLogTransactions.sortBy(_.transactionID).sortBy(_.timestamp).toList
      val (firstTransaction, otherTransactions) = (orderedCommitLogTransactionsByTimestamp.head, orderedCommitLogTransactionsByTimestamp.tail)
      val processedFirstTransaction = processFirstTransaction(firstTransaction)
      process(processedFirstTransaction :: otherTransactions)
    } else {
      processFirstTransaction(commitLogTransactions.head)
    }
  }
}
