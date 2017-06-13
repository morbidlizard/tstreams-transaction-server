package com.bwsw.tstreamstransactionserver.netty.server.commitLogService.bookkeeper

import org.apache.bookkeeper.client.LedgerHandle

class Batch(ledgerHandle: LedgerHandle) {

  def put(data: Array[Byte]): Unit =  {
    ledgerHandle.addEntry(data)
  }

  def close(): Unit = {
    ledgerHandle.close()
  }

}
