package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.{PutConsumerCheckpoint, PutTransaction, PutTransactions}

object RecordType
  extends Enumeration {

  val Timestamp                 = Value(0)
  val TransactionData           = Value(1)
  val PutTransactionType        = Value(2)
  val PutTransactionsType       = Value(3)
  val PutConsumerCheckpointType = Value(4)

  def deserializePutTransaction(message: Array[Byte]): PutTransaction.Args =
    Protocol.PutTransaction.decodeRequest(message)

  def deserializePutTransactions(message: Array[Byte]): PutTransactions.Args =
    Protocol.PutTransactions.decodeRequest(message)

  def deserializePutConsumerCheckpoint(message: Array[Byte]): PutConsumerCheckpoint.Args =
    Protocol.PutConsumerCheckpoint.decodeRequest(message)
}
