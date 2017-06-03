package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler


import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionRecord, ProducerTransactionValue}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates._

import scala.annotation.tailrec

trait TransactionStateHandler {
  private[transactionMetadataService] def transitProducerTransactionToInvalidState(txn: ProducerTransactionRecord) = {
    ProducerTransactionRecord(
      ProducerTransactionKey(txn.stream, txn.partition, txn.transactionID),
      ProducerTransactionValue(Invalid, 0, 0L, txn.timestamp)
    )
  }

  private def isThisProducerTransactionExpired(currentTxn: ProducerTransactionRecord,
                                               nextTxn: ProducerTransactionRecord): Boolean = {
    scala.math.abs(currentTxn.timestamp + currentTxn.ttl) <= nextTxn.timestamp
  }

  @throws[IllegalArgumentException]
  private def transitProducerTransactionToNewState(currentTxn: ProducerTransactionRecord,
                                                   nextTxn: ProducerTransactionRecord): Option[ProducerTransactionRecord] = {
    (currentTxn.state, nextTxn.state) match {
      case (Opened, Opened) =>
        Some(currentTxn)

      case (Opened, Updated) => Some(
        if (isThisProducerTransactionExpired(currentTxn, nextTxn))
          transitProducerTransactionToInvalidState(currentTxn)
        else
          ProducerTransactionRecord(
            com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionKey(nextTxn.stream, nextTxn.partition, nextTxn.transactionID),
            ProducerTransactionValue(Opened, nextTxn.quantity, nextTxn.ttl, nextTxn.timestamp)
          )
      )

      case (Opened, Cancel) =>
        Some(transitProducerTransactionToInvalidState(currentTxn))


      case (Opened, Invalid) =>
        None
//        throw new IllegalArgumentException("An opened transaction can transit to the Invalid state by Cancel state only!")

      case (Opened, Checkpointed) =>
        if (isThisProducerTransactionExpired(currentTxn, nextTxn))
          Some(transitProducerTransactionToInvalidState(currentTxn))
        else
          Some(nextTxn)

      case (Updated, _) =>
        None
//        throw new IllegalArgumentException("A transaction with Updated state can't be a root of transactions chain.")
      case (Cancel, _) =>
        None
//        throw new IllegalArgumentException("A transaction with Cancel state can't be a root of transactions chain.")

      case (Invalid, _) =>
        Some(currentTxn)
      case (Checkpointed, _) =>
        Some(currentTxn)

      case (thisState, thatState) =>
        None
//        throw new IllegalArgumentException(s"Transition from $thisState to $thatState should be implemented.(Unknown states)")
    }
  }

  @tailrec
  private def process(txns: Seq[ProducerTransactionRecord]): Option[ProducerTransactionRecord] = {
    if (txns.isEmpty)
      None
    else if (txns.length == 1) {
      val rootTransaction = txns.head
      rootTransaction.state match {
        case Opened =>
          Some(rootTransaction)
        case state: TransactionStates =>
          None
      }
    }
    //          throw new IllegalArgumentException(
    //            s"A transaction ${head.transactionID} with $state state can't be a root of transactions chain."
    //          )

    else {
      val rootTransaction =
        txns.head

      val followingTransactionToProcess =
        txns.tail.tail

      val nextTransaction =
        txns.tail.head

      transitProducerTransactionToNewState(rootTransaction, nextTransaction) match {
        case None =>
          None
        case Some(transactionWithNewState) =>
          if (transactionWithNewState.state == Checkpointed ||
            transactionWithNewState.state == Invalid
          ) {
            Some(transactionWithNewState)
          }
          else {
            process(transactionWithNewState +: followingTransactionToProcess)
          }
      }
    }
  }



//    txns match {
//    case Nil => throw new IllegalArgumentException
//    case head :: Nil => head.state match {
//      case Opened => head
//      case state: TransactionStates =>
//        throw new IllegalArgumentException(
//          s"A transaction ${head.transactionID} with $state state can't be a root of transactions chain."
//        )
//    }
//    case head :: next :: Nil =>
//      if ((head.state == Invalid) || (head.state == Checkpointed)) head
//      else transitProducerTransactionToNewState(head, next)
//    case head :: next :: tail =>
//      if ((head.state == Invalid) || (head.state == Checkpointed)) head
//      else process(transitProducerTransactionToNewState(head, next) :: tail)
//  }


  @throws[IllegalArgumentException]
  final def transitProducerTransactionToNewState(transactionPersistedInBerkeleyDB: ProducerTransactionRecord,
                                                 commitLogTransactions: Seq[ProducerTransactionRecord]): Option[ProducerTransactionRecord] = {

    val orderedCommitLogTransactions =
      commitLogTransactions
        //        .sortBy(_.transactionID)
        .sortBy(_.state.value)
        .sortBy(_.timestamp)
        .toList

    process(transactionPersistedInBerkeleyDB :: orderedCommitLogTransactions)
  }

  @throws[IllegalArgumentException]
  final def isTransactionCanBeRootOfChain(txn: ProducerTransactionRecord): Boolean = {
    if (txn.state == TransactionStates.Opened)
      true
    else
      false
  }

  @throws[IllegalArgumentException]
  final def transitProducerTransactionToNewState(commitLogTransactions: Seq[ProducerTransactionRecord]): Option[ProducerTransactionRecord] = {
    val orderedCommitLogTransactions =
      commitLogTransactions
        //        .sortBy(_.transactionID)
        .sortBy(_.state.value)
        .sortBy(_.timestamp)
        .toList

    if (orderedCommitLogTransactions.nonEmpty &&
      isTransactionCanBeRootOfChain(orderedCommitLogTransactions.head)
    )
      process(orderedCommitLogTransactions)
    else
      None
  }
}
