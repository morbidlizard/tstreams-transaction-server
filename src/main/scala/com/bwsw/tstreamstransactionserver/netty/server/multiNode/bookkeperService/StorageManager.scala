package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

abstract class StorageManager() {
  def createLedger(): LedgerHandle

  def openLedger(id: Long): Option[LedgerHandle]

  def deleteLedger(id: Long): Boolean

  def isClosed(id: Long): Boolean
}
