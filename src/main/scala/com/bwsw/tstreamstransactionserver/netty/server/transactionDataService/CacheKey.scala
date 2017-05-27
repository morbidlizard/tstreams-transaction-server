package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService


case class CacheKey(streamID: Int, partition: Int, transaction: Long, dataID: Int) {
  override def toString: String = s"$partition $transaction"
}