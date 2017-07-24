package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionRecord, ProducerTransactionValue}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Cancel, Checkpointed, Invalid, Opened, Updated}

import scala.annotation.tailrec


object ProducerTransactionState {
  def apply(producerTransactionRecord: ProducerTransactionRecord): ProducerTransactionState = {
    producerTransactionRecord.state match {
      case Opened =>
        new OpenedTransactionState(producerTransactionRecord)
      case Updated =>
        new UpdatedTransactionState(producerTransactionRecord)
      case Cancel =>
        new CanceledTransactionState(producerTransactionRecord)
      case Invalid =>
        new InvalidTransactionState(producerTransactionRecord)
      case Checkpointed =>
        new CheckpointedTransactionState(producerTransactionRecord)
      case _ =>
        new UndefinedTransactionState(producerTransactionRecord)
    }
  }

  def checkFinalStateOnCorrectness(producerTransactionState: ProducerTransactionState): Boolean = {
    producerTransactionState match {
      case _: OpenedTransactionState =>
        true
      case _: InvalidTransactionState =>
        true
      case _: CheckpointedTransactionState =>
        true
      case _ =>
        false
    }
  }

  final def transiteTransactionsToFinalState(records: Seq[ProducerTransactionRecord],
                                             onTransitionDo: ProducerTransactionRecord => Unit): Option[ProducerTransactionState] = {
    @tailrec
    def go(records: List[ProducerTransactionRecord],
           currentState: ProducerTransactionState): ProducerTransactionState = {
      records match {
        case Nil =>
          onTransitionDo(currentState.producerTransactionRecord)
          currentState
        case head :: tail =>
          val newState = currentState.handle(
            ProducerTransactionState(head)
          )
          onTransitionDo(newState.producerTransactionRecord)
          if (newState == currentState ||
            newState.isInstanceOf[UndefinedTransactionState]) {
            currentState
          }
          else {
            go(tail, newState)
          }
      }
    }

    val recordsLst =
      records.toList
    recordsLst
      .headOption
      .map(rootRecord =>
        go(recordsLst.tail, ProducerTransactionState(rootRecord))
      )
  }

  final def transiteTransactionsToFinalState(records: Seq[ProducerTransactionRecord]): Option[ProducerTransactionState] = {
    transiteTransactionsToFinalState(records, _ => {})
  }
}

abstract sealed class ProducerTransactionState (val producerTransactionRecord: ProducerTransactionRecord)
{
  def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState
}


class OpenedTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord)
{

  private def transitProducerTransactionToInvalidState() = {
    val txn = producerTransactionRecord
    ProducerTransactionRecord(
      ProducerTransactionKey(txn.stream, txn.partition, txn.transactionID),
      ProducerTransactionValue(Invalid, 0, 0L, txn.timestamp)
    )
  }

  private def isThisProducerTransactionExpired(that: ProducerTransactionRecord): Boolean = {
    val expirationPoint = producerTransactionRecord.timestamp + producerTransactionRecord.ttl
    scala.math.abs(expirationPoint) <= that.timestamp
  }

  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    producerTransactionState match {
      case _: OpenedTransactionState =>
        this

      case that: UpdatedTransactionState =>
        val thatTxn = that.producerTransactionRecord

        val transitedProducerTransaction =
          if (isThisProducerTransactionExpired(thatTxn))
            transitProducerTransactionToInvalidState()
          else {
            ProducerTransactionRecord(
              ProducerTransactionKey(thatTxn.stream, thatTxn.partition, thatTxn.transactionID),
              ProducerTransactionValue(Opened, thatTxn.quantity, thatTxn.ttl, thatTxn.timestamp)
            )
          }
        ProducerTransactionState(transitedProducerTransaction)

      case _: CanceledTransactionState =>
        ProducerTransactionState(transitProducerTransactionToInvalidState())

      case that: CheckpointedTransactionState =>
        val thatTxn = that.producerTransactionRecord

        if (isThisProducerTransactionExpired(thatTxn))
          ProducerTransactionState(transitProducerTransactionToInvalidState())
        else {
          that
        }

      case that =>
        new UndefinedTransactionState(that.producerTransactionRecord)
    }
}

class UpdatedTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord){
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    producerTransactionState match {
      case that =>
        new UndefinedTransactionState(that.producerTransactionRecord)
    }
}

class CanceledTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord){
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    producerTransactionState match {
      case that =>
        new UndefinedTransactionState(that.producerTransactionRecord)
    }
}

class InvalidTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord){
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    this
}

class CheckpointedTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord){
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    this
}

class UndefinedTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord){
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    producerTransactionState match {
      case that =>
        new UndefinedTransactionState(that.producerTransactionRecord)
    }
}
