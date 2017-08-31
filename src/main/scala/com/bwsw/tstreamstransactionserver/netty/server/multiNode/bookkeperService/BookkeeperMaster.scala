package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.util
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerIsSlaveException
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.TimestampRecord
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper.DigestType
import org.apache.bookkeeper.client.{BKException, BookKeeper}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class BookkeeperMaster(bookKeeper: BookKeeper,
                       master: LeaderSelectorInterface,
                       bookkeeperOptions: BookkeeperOptions,
                       zkTreeListLedger: LongZookeeperTreeList,
                       timeBetweenCreationOfLedgers: Int)
  extends Runnable {

  private val openedLedgers =
    new java.util.concurrent.LinkedBlockingQueue[org.apache.bookkeeper.client.LedgerHandle](10)

  private def closeLastLedger(): Unit = {
    zkTreeListLedger
      .lastEntityID
      .foreach { id =>
        closeLedger(id)
      }
  }

  private def closeLedger(ledgerHandle: org.apache.bookkeeper.client.LedgerHandle): Unit = {
    this.synchronized(
      ledgerHandle.close()
    )
  }

  private def closeLedger(id: Long): Unit = {
    scala.util.Try(bookKeeper
      .openLedger(id, BookKeeper.DigestType.MAC, bookkeeperOptions.password)
    ) match {
      case scala.util.Success(_) =>
      case scala.util.Failure(throwable) => throwable match {
        case _: BKException.BKLedgerRecoveryException =>
        case _: Throwable =>
          throw throwable
      }
    }
  }
  private var isSorted = 0L
  private final def whileLeaderDo() = {

    var lastAccessTimes = 0L

    @tailrec
    def onBeingLeaderDo(): Unit = {
      if (master.hasLeadership) {
        if ((System.currentTimeMillis() - lastAccessTimes) <= timeBetweenCreationOfLedgers) {
          val timeToWait = math.abs(timeBetweenCreationOfLedgers -
            (System.currentTimeMillis() - lastAccessTimes)
          )
          TimeUnit.MILLISECONDS.sleep(timeToWait)
          onBeingLeaderDo()
        }
        else {
          lastAccessTimes = System.currentTimeMillis()
          scala.util.Try {
            ledgerHandleToWrite(
              bookkeeperOptions.ensembleNumber,
              bookkeeperOptions.writeQuorumNumber,
              bookkeeperOptions.ackQuorumNumber,
              BookKeeper.DigestType.MAC
            )
          }.map { ledgerHandle =>

            val ledgerNumber = openedLedgers.size()
            if (ledgerNumber > 1) {
              val ledgerRequest = openedLedgers.poll()
              closeLedger(ledgerRequest)
            }

            val timestamp = System.currentTimeMillis()
            if (isSorted > timestamp) println("Bad master")
            isSorted = timestamp
//            timestamps += timestamp
//            println((timestamps zip timestamps.sorted)
//              .forall{case (a,b) => a == b})
            val timestampRecord =
              new TimestampRecord(timestamp)



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
      bookkeeperOptions.password
    )
  }

  @tailrec
  private def retryToGetLedger: Either[ServerIsSlaveException, org.apache.bookkeeper.client.LedgerHandle] = {
    val openedLedger = openedLedgers.peek()
    if (openedLedger == null) {
      if (master.hasLeadership) {
        TimeUnit.MILLISECONDS.sleep(10)
        retryToGetLedger
      }
      else {
        Left(new ServerIsSlaveException)
      }
    }
    else {
      Right(openedLedger)
    }
  }

  private def lead(): Unit = {
    closeLastLedger()
    whileLeaderDo()
  }

  @throws[Exception]
  def doOperationWithCurrentWriteLedger[T](operate: Either[ServerIsSlaveException, org.apache.bookkeeper.client.LedgerHandle] => T): T = {

    if (master.hasLeadership) {
     this.synchronized {
       try {
         val ledgerHandle = retryToGetLedger
         operate(ledgerHandle)
       }
       catch {
         case throwable: Throwable =>
           throw throwable
       }
     }
    } else {
      operate(Left(new ServerIsSlaveException))
    }
  }

  override def run(): Unit = {
    try {
      while (true) {
        if (master.hasLeadership)
          lead()
        else {
          val ledgersRequests =
            new util.LinkedList[org.apache.bookkeeper.client.LedgerHandle]()
          openedLedgers.drainTo(ledgersRequests)
          ledgersRequests.forEach(closeLedger)
        }
      }
    }
    catch {
      case _: java.lang.InterruptedException =>
        Thread.currentThread().interrupt()
    }
  }

}
