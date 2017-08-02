package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

abstract class LedgerManager() {
  def createLedger(): LedgerHandle

  def openLedger(id: Long): Option[LedgerHandle]

  def deleteLedger(id: Long): Boolean

  def isClosed(id: Long): Boolean
}
