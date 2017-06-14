package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.record

object RecordType
  extends Enumeration
{
  val ProducerTransaction, ConsumerTransaction, Transaction, TransactionSeq = Value
}
