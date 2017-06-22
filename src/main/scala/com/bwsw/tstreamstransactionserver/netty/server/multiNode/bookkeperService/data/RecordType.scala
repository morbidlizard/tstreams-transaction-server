package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data

object RecordType
  extends Enumeration
{
  val ProducerTransaction, ConsumerTransaction, Transaction, TransactionSeq, TransactionData, Timestamp = Value
}
