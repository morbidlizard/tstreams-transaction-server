package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

abstract class StorageManager()
{
  def addLedger(): Ledger
  def getLedger(id: Long): Option[Ledger]
  def deleteLedger(id: Long): Boolean
}
