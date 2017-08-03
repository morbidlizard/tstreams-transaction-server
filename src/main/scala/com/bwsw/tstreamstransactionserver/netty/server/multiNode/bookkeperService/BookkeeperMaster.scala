package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.util.concurrent.BlockingQueue

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.TimestampRecord
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import org.apache.bookkeeper.client.BookKeeper.DigestType
import org.apache.bookkeeper.client.{BKException, BookKeeper}

import scala.annotation.tailrec

class BookkeeperMaster(bookKeeper: BookKeeper,
                       master: LeaderSelectorInterface,
                       replicationConfig: ReplicationConfig,
                       zkTreeListLedger: ZookeeperTreeListLong,
                       password: Array[Byte],
                       timeBetweenCreationOfLedgers: Int,
                       openedLedgers: BlockingQueue[org.apache.bookkeeper.client.LedgerHandle])
  extends Runnable {

  override def run(): Unit = {
    while (true) {
      if (master.hasLeadership)
        lead()
    }
  }

  def lead(): Unit = {
    closeLastLedger()
    whileLeaderDo()
  }

  private def closeLastLedger(): Unit = {
    zkTreeListLedger
      .lastEntityID
      .foreach { id =>
        closeLedger(id)
      }
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

  private final def whileLeaderDo() = {

    var lastAccessTimes = 0L

    @tailrec
    def onBeingLeaderDo(): Unit = {
      if (master.hasLeadership) {
        if ((System.currentTimeMillis() - lastAccessTimes) <= timeBetweenCreationOfLedgers) {
          onBeingLeaderDo()
        }
        else {
          lastAccessTimes = System.currentTimeMillis()
          scala.util.Try {
            ledgerHandleToWrite(
              replicationConfig.ensembleNumber,
              replicationConfig.writeQuorumNumber,
              replicationConfig.ackQuorumNumber,
              BookKeeper.DigestType.MAC
            )
          }.map { ledgerHandle =>
            val timestampRecord =
              new TimestampRecord(System.currentTimeMillis())

            ledgerHandle.addEntry(timestampRecord.toByteArray)

            zkTreeListLedger.createNode(
              ledgerHandle.getId
            )

            openedLedgers.add(ledgerHandle)
          }
          onBeingLeaderDo()
        }
      }
    }

    onBeingLeaderDo()
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
}
