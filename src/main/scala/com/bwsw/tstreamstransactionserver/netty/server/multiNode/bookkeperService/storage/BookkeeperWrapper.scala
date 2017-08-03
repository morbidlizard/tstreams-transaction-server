package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{LedgerHandle, ReplicationConfig, LedgerManager}
import org.apache.bookkeeper.client.BookKeeper

import scala.util.Try

class BookkeeperWrapper(bookKeeper: BookKeeper,
                        replicationConfig: ReplicationConfig,
                        password: Array[Byte])
  extends LedgerManager {

  override def createLedger(): LedgerHandle = {
    val ledgerHandle = bookKeeper.createLedger(
      replicationConfig.ensembleNumber,
      replicationConfig.writeQuorumNumber,
      replicationConfig.ackQuorumNumber,
      BookKeeper.DigestType.MAC,
      password
    )
    new BookKeeperLedgerHandleWrapper(ledgerHandle)
  }

  override def openLedger(id: Long): Option[LedgerHandle] = {
    val ledgerHandleTry = Try(bookKeeper.openLedgerNoRecovery(
      id,
      BookKeeper.DigestType.MAC,
      password
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
