package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.{PutConsumerCheckpoint, PutProducerStateWithData, PutTransaction, PutTransactions}

object RecordType
  extends Enumeration {

  val Timestamp                    = Value(0)
  val TransactionData              = Value(1)
  val PutProducerStateWithDataType = Value(2)
  val PutTransactionType           = Value(3)
  val PutTransactionsType          = Value(4)
  val PutConsumerCheckpointType    = Value(5)

  def deserializePutTransaction(message: Array[Byte]): PutTransaction.Args =
    Protocol.PutTransaction.decodeRequest(message)

  def deserializePutTransactions(message: Array[Byte]): PutTransactions.Args =
    Protocol.PutTransactions.decodeRequest(message)

  def deserializePutConsumerCheckpoint(message: Array[Byte]): PutConsumerCheckpoint.Args =
    Protocol.PutConsumerCheckpoint.decodeRequest(message)

  def deserializePutProducerStateWithData(message: Array[Byte]): PutProducerStateWithData.Args =
    Protocol.PutProducerStateWithData.decodeRequest(message)
}
