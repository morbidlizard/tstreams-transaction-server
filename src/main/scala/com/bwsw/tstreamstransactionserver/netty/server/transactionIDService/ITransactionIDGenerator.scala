package com.bwsw.tstreamstransactionserver.netty.server.transactionIDService

trait ITransactionIDGenerator {
  def getTransaction(): Long

  def getTransaction(timestamp: Long): Long
}
