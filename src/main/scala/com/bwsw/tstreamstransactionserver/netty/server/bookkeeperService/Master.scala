package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

import java.util.concurrent.BlockingQueue

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.TimestampRecord
import org.apache.bookkeeper.client.BookKeeper.DigestType
import org.apache.bookkeeper.client.{BKException, BookKeeper, LedgerHandle}
import org.apache.curator.framework.CuratorFramework

import scala.annotation.tailrec

class Master(client: CuratorFramework,
             bookKeeper: BookKeeper,
             master: Electable,
             replicationConfig: ReplicationConfig,
             zkTreeListLedger: ZookeeperTreeListLong,
             password: Array[Byte],
             timeBetweenCreationOfLedgers: Int,
             openedLedgers: BlockingQueue[LedgerHandle])
{

  def lead(): Unit = {
    closeLastLedger()
    whileLeaderDo()
  }

  private def closeLastLedger(): Unit = {
    zkTreeListLedger
      .lastEntityID
      .foreach(id =>
        closeLedger(id)
      )
  }

  private def closeLedger(id: Long): Unit = {
    scala.util.Try(bookKeeper
      .openLedger(id, BookKeeper.DigestType.MAC, password)
    ) match {
      case scala.util.Success(_) =>
      case scala.util.Failure(throwable) => throwable match {
        case _: BKException.BKLedgerRecoveryException =>
        case _: Throwable =>
          throw throwable
      }
    }
  }

  private def ledgerHandleToWrite(ensembleNumber: Int,
                                  writeQuorumNumber: Int,
                                  ackQuorumNumber: Int,
                                  digestType: DigestType
                                 ) = {
    bookKeeper.createLedger(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      digestType,
      password
    )
  }

  private final def whileLeaderDo() =
  {

    var lastAccessTimes = 0L
    @tailrec
    def onBeingLeaderDo(): Unit =
    {
      if (master.hasLeadership) {
        if ((System.currentTimeMillis() - lastAccessTimes) <= timeBetweenCreationOfLedgers) {
          onBeingLeaderDo()
        }
        else {
          lastAccessTimes = System.currentTimeMillis()
          val ledgerHandle = ledgerHandleToWrite(
            replicationConfig.ensembleNumber,
            replicationConfig.writeQuorumNumber,
            replicationConfig.ackQuorumNumber,
            BookKeeper.DigestType.MAC
          )

          ledgerHandle.addEntry(
            new TimestampRecord(System.currentTimeMillis())
              .toByteArray
          )

          zkTreeListLedger.createNode(
            ledgerHandle.getId
          )
          openedLedgers.add(ledgerHandle)
          onBeingLeaderDo()
        }
      }
    }
    onBeingLeaderDo()
  }
}
