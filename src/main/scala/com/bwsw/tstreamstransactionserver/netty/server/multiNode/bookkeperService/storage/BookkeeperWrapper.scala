package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{LedgerHandle, LedgerManager}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper

import scala.util.Try

class BookkeeperWrapper(bookKeeper: BookKeeper,
                        bookkeeperOptions: BookkeeperOptions)
  extends LedgerManager {

  override def createLedger(): LedgerHandle = {
    val ledgerHandle = bookKeeper.createLedger(
      bookkeeperOptions.ensembleNumber,
      bookkeeperOptions.writeQuorumNumber,
      bookkeeperOptions.ackQuorumNumber,
      BookKeeper.DigestType.MAC,
      bookkeeperOptions.password
    )
    new BookKeeperLedgerHandleWrapper(ledgerHandle)
  }

  override def openLedger(id: Long): Option[LedgerHandle] = {
    val ledgerHandleTry = Try(bookKeeper.openLedgerNoRecovery(
      id,
      BookKeeper.DigestType.MAC,
      bookkeeperOptions.password
    ))
    ledgerHandleTry.map(ledgerHandle =>
      new BookKeeperLedgerHandleWrapper(ledgerHandle)
    ).toOption
  }

  override def deleteLedger(id: Long): Boolean = {
    Try(bookKeeper.deleteLedger(id)).isSuccess
  }

  override def isClosed(id: Long): Boolean = {
    Try(bookKeeper.isClosed(id)).getOrElse(false)
  }
}
