package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionWithoutKey}
import transactionService.rpc.TransactionStates
import transactionService.rpc.TransactionStates._

import scala.annotation.tailrec

trait TransactionStateHandler extends LastTransactionStreamPartition {
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
  private final def transitProducerTransactionToNewState(currentTxn: ProducerTransactionKey, nextTxn: ProducerTransactionKey): ProducerTransactionKey = {
    (currentTxn.state, nextTxn.state) match {
      case (Opened, Opened) => currentTxn

      case (Opened, Updated) =>
        if (isThisProducerTransactionExpired(currentTxn, nextTxn)) transiteProducerTransactiontoInvalidState(currentTxn)
        else
          ProducerTransactionKey(
            com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.Key(nextTxn.stream, nextTxn.partition, nextTxn.transactionID),
            ProducerTransactionWithoutKey(Opened, nextTxn.quantity, nextTxn.ttl, nextTxn.timestamp)
          )

      case (Opened, Cancel) => {
        transiteProducerTransactiontoInvalidState(currentTxn)
      }

      case (Opened, Invalid) => throw new IllegalArgumentException("An opened transaction can transit to the Invalid state by Cancel state only!")

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
      else transitProducerTransactionToNewState(head, next)
    case head::next::tail =>
      if ((head.state == Invalid) || (head.state == Checkpointed)) head
      else process(transitProducerTransactionToNewState(head, next) :: tail)
  }

  @throws[IllegalArgumentException]
  final def transitProducerTransactionToNewState(dbTransaction: ProducerTransactionKey, commitLogTransactions: Seq[ProducerTransactionKey]): ProducerTransactionKey = {
    process(dbTransaction :: commitLogTransactions.sortBy(_.timestamp).toList)
  }

  @throws[IllegalArgumentException]
  final def transitProducerTransactionToNewState(commitLogTransactions: Seq[ProducerTransactionKey]): ProducerTransactionKey = {
    process(commitLogTransactions.sortBy(_.timestamp).toList)
  }
}
