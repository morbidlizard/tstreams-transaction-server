package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data

object RecordType
  extends Enumeration
{
  val Timestamp, TransactionData, ProducerTransaction, ConsumerTransaction, Transaction, TransactionSeq = Value
}
