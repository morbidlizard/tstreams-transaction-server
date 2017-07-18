package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.rpc.TransactionService._

object RecordType
  extends Enumeration {

  val Timestamp                       = Value(0)
  val PutTransactionDataType          = Value(1)
  val PutSimpleTransactionAndDataType = Value(2)
  val PutProducerStateWithDataType    = Value(3)
  val PutTransactionType              = Value(4)
  val PutTransactionsType             = Value(5)
  val PutConsumerCheckpointType       = Value(6)

  def deserializePutTransactionData(message: Array[Byte]): PutTransactionData.Args =
    Protocol.PutTransactionData.decodeRequest(message)

  def deserializePutTransaction(message: Array[Byte]): PutTransaction.Args =
    Protocol.PutTransaction.decodeRequest(message)

  def deserializePutTransactions(message: Array[Byte]): PutTransactions.Args =
    Protocol.PutTransactions.decodeRequest(message)

  def deserializePutConsumerCheckpoint(message: Array[Byte]): PutConsumerCheckpoint.Args =
    Protocol.PutConsumerCheckpoint.decodeRequest(message)

  def deserializePutProducerStateWithData(message: Array[Byte]): PutProducerStateWithData.Args =
    Protocol.PutProducerStateWithData.decodeRequest(message)
}
