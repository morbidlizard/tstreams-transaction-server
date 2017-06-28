package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

abstract class StorageManager()
{
  def addLedger(): LedgerHandle
  def getLedgerHandle(id: Long): Option[LedgerHandle]
  def deleteLedger(id: Long): Boolean
}
