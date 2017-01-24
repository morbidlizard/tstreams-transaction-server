package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService

case class StorageName(stream: String) extends AnyVal{
  override def toString: String = stream
}
