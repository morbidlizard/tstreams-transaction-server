package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionWithoutKey}
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

  private final def transitProducerTransactionToInvalidState(txn: ProducerTransactionKey) = {
    ProducerTransactionKey(
      com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.Key(txn.stream, txn.partition, txn.transactionID),
      ProducerTransactionWithoutKey(Invalid, txn.quantity, 0L, txn.timestamp)
    )
  }

  private final def isThisProducerTransactionExpired(currentTxn: ProducerTransactionKey, nextTxn: ProducerTransactionKey): Boolean = {
    scala.math.abs(currentTxn.timestamp + TimeUnit.SECONDS.toMillis(currentTxn.ttl)) <= nextTxn.timestamp
  }

  @throws[IllegalArgumentException]
  private final def transitProducerTransactionToNewState(currentTxn: ProducerTransactionKey, nextTxn: ProducerTransactionKey): ProducerTransactionKey = {
    (currentTxn.state, nextTxn.state) match {
      case (Opened, Opened) => currentTxn

      case (Opened, Updated) =>
        if (isThisProducerTransactionExpired(currentTxn, nextTxn)) transitProducerTransactionToInvalidState(currentTxn)
        else
          ProducerTransactionKey(
            com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.Key(nextTxn.stream, nextTxn.partition, nextTxn.transactionID),
            ProducerTransactionWithoutKey(Opened, nextTxn.quantity, nextTxn.ttl, nextTxn.timestamp)
          )

      case (Opened, Cancel) => {
        transitProducerTransactionToInvalidState(currentTxn)
      }

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

      case (_, _) => throw new IllegalArgumentException("Unknown States should be implemented")
    }
  }


  @tailrec
  @throws[IllegalArgumentException]
  private final def process(txns: List[ProducerTransactionKey]): ProducerTransactionKey = txns match {
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
  private final def processFirstTransaction(transaction: ProducerTransactionKey) = {
    process(transaction::Nil)
  }

  @throws[IllegalArgumentException]
  final def transitProducerTransactionToNewState(transactionPersistedInBerkeleyDB: ProducerTransactionKey, commitLogTransactions: Seq[ProducerTransactionKey]): ProducerTransactionKey = {
    val firstTransaction = processFirstTransaction(transactionPersistedInBerkeleyDB)
    process(firstTransaction :: commitLogTransactions.sortBy(_.transactionID).sortBy(_.timestamp).toList)
  }

  @throws[IllegalArgumentException]
  final def transitProducerTransactionToNewState(commitLogTransactions: Seq[ProducerTransactionKey]): ProducerTransactionKey = {
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
