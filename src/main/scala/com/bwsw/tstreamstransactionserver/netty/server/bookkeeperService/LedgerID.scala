package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

class LedgerID(val ledgerId: Long)
  extends AnyVal

object LedgerID{
  def apply(ledgerId: Long): LedgerID =
    new LedgerID(ledgerId)
}
