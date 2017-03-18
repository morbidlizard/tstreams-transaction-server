package com.bwsw.tstreamstransactionserver.netty

import transactionService.rpc.{ProducerTransaction, TransactionStates}

object Shared {
  val nullProducerTransaction = ProducerTransaction("null", -1, -1, TransactionStates.Invalid, -1, -1)
}
