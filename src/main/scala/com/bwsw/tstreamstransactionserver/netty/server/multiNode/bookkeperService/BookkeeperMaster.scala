package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.util
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerIsSlaveException
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.TimestampRecord
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper.DigestType
import org.apache.bookkeeper.client.{BKException, BookKeeper}

import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}

class BookkeeperMaster(bookKeeper: BookKeeper,
                       master: LeaderSelectorInterface,
                       bookkeeperOptions: BookkeeperOptions,
                       zkTreeListLedger: ZookeeperTreeListLong,
                       timeBetweenCreationOfLedgers: Int)
  extends Runnable {


  private val openedLedgers =
    new java.util.concurrent.LinkedBlockingQueue[LedgerRequests](10)

  private def closeLastLedger(): Unit = {
    zkTreeListLedger
      .lastEntityID
      .foreach { id =>
        closeLedger(id)
      }
  }

  private val immediateContext = new ExecutionContext {
    def execute(runnable: Runnable) {
      runnable.run()
    }
    def reportFailure(cause: Throwable) {}
  }

  private def closeLedger(ledgerRequests: LedgerRequests): Unit = {
    implicit val context = immediateContext
    val requests = ledgerRequests
      .requests
      .asScala
      .withFilter(request => !request.isCompleted)
      .map(_.future)
    Future
      .sequence(requests)
      .onComplete(_ => ledgerRequests.ledger.close())
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

            val timestampRecord =
              new TimestampRecord(System.currentTimeMillis())

            ledgerHandle.addEntry(timestampRecord.toByteArray)

            zkTreeListLedger.createNode(
              ledgerHandle.getId
            )


            openedLedgers.add(LedgerRequests(ledgerHandle))
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
  private def retryToGetLedger: Either[ServerIsSlaveException, LedgerRequests] = {
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
  def doOperationWithCurrentWriteLedger(operate: Either[ServerIsSlaveException, org.apache.bookkeeper.client.LedgerHandle] => Promise[_]): Unit = {
    if (master.hasLeadership) {
      try {
        val ledgerRequests =
          retryToGetLedger

        val currentLedgerHandle =
          ledgerRequests.map(_.ledger)

        ledgerRequests.foreach(ledgerRequest =>
          ledgerRequest.requests.add(operate(currentLedgerHandle))
        )
      }
      catch {
        case _: Throwable =>
//          throw throwable
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
            new util.LinkedList[LedgerRequests]()
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
