/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler


import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionRecord, ProducerTransactionValue}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates._

import scala.annotation.tailrec

object TransactionStateHandler {
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


  final def transitProducerTransactionToNewState(transactionPersistedInDB: ProducerTransactionRecord,
                                                 commitLogTransactions: Seq[ProducerTransactionRecord]): Option[ProducerTransactionRecord] = {


    if (isTransactionCanBeRootOfChain(transactionPersistedInDB)) {
      val orderedCommitLogTransactions = commitLogTransactions.sorted

      process(transactionPersistedInDB +: orderedCommitLogTransactions)
    } else {
      None
    }
  }

  private final def isTransactionCanBeRootOfChain(txn: ProducerTransactionRecord): Boolean = {
    if (txn.state == TransactionStates.Opened)
      true
    else
      false
  }

  final def transitProducerTransactionToNewState(commitLogTransactions: Seq[ProducerTransactionRecord]): Option[ProducerTransactionRecord] = {
    val orderedCommitLogTransactions = commitLogTransactions.sorted

    if (orderedCommitLogTransactions.nonEmpty &&
      isTransactionCanBeRootOfChain(orderedCommitLogTransactions.head)
    )
      process(orderedCommitLogTransactions)
    else
      None
  }
}
