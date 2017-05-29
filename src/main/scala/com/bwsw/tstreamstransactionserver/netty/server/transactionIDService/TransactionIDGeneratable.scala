package com.bwsw.tstreamstransactionserver.netty.server.transactionIDService

trait TransactionIDGeneratable {
  def getTransaction(): Long

  def getTransaction(timestamp: Long): Long
}
