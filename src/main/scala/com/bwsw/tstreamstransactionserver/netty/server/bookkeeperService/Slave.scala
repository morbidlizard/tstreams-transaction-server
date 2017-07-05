package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

import java.util.concurrent.{BlockingQueue, TimeUnit}

import org.apache.bookkeeper.client.{BookKeeper, LedgerHandle}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import Utils._

import scala.annotation.tailrec

class Slave(client: CuratorFramework,
            bookKeeper: BookKeeper,
            slave: Electable,
            password: Array[Byte],
            timeBetweenCreationOfLedgers: Int,
            closedLedgers: BlockingQueue[LedgerHandle])
{

  def follow(skipPast: LedgerID): Unit = {
//    val ledgers =
//      retrieveLedgersUntilNodeDoesntExist(skipPast)
//
//      retrieveUpcomingLedgers(ledgers,  skipPast)
  }

  @tailrec
  private final def monitorLedgerUntilItIsCompleted(ledger: Long,
                                                    lastLedgerAndItsLastRecordSeen: LedgerID,
                                                    isLedgerCompleted: Boolean): LedgerID = {
    if (isLedgerCompleted || slave.hasLeadership) {
      lastLedgerAndItsLastRecordSeen
    } else {
      val ledgerHandle = bookKeeper.openLedgerNoRecovery(
        ledger,
        BookKeeper.DigestType.MAC,
        password
      )


      val isLedgerCompleted = ledgerHandle.isClosed

      val lastProcessedLedger =
        if (isLedgerCompleted && (lastLedgerAndItsLastRecordSeen.ledgerId < ledgerHandle.getId)) {
          closedLedgers.add(ledgerHandle)
          LedgerID(ledgerHandle.getId)
        }
        else {
          lastLedgerAndItsLastRecordSeen
        }

      TimeUnit.MILLISECONDS.sleep(timeBetweenCreationOfLedgers)
      monitorLedgerUntilItIsCompleted(
        ledger,
        lastProcessedLedger,
        isLedgerCompleted
      )
    }
  }



  private final def readUntilWeAreSlave(ledgers: Array[Long],
                                        lastLedgerAndItsLastRecordSeen: LedgerID
                                       ): LedgerID = {
    ledgers.foldRight(lastLedgerAndItsLastRecordSeen)((ledger, lastLedgerAndItsLastRecordSeen) =>
      monitorLedgerUntilItIsCompleted(ledger,
        lastLedgerAndItsLastRecordSeen,
        isLedgerCompleted = false
      )
    )
  }
}