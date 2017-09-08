package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerIsSlaveException
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client
import org.apache.bookkeeper.client.BookKeeper.DigestType
import org.apache.bookkeeper.client.{AsyncCallback, BKException, BookKeeper, LedgerMetadata}

import scala.annotation.tailrec


class BookkeeperMaster(bookKeeper: BookKeeper,
                       master: LeaderSelectorInterface,
                       bookkeeperOptions: BookkeeperOptions,
                       zkTreeListLedger: LongZookeeperTreeList,
                       timeBetweenCreationOfLedgers: Int)
  extends Runnable {

  private val lock = new ReentrantReadWriteLock()
  @volatile private var currentOpenedLedger: org.apache.bookkeeper.client.LedgerHandle = _


  private def closeLastLedger(): Unit = {
    zkTreeListLedger
      .lastEntityID
      .foreach { id =>
        closeLedger(id)
      }
  }

  private def closeLedger(ledgerHandle: org.apache.bookkeeper.client.LedgerHandle): Unit = {
    scala.util.Try {
      ledgerHandle.close()
    }
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

            val previousOpenedLedger = currentOpenedLedger

            zkTreeListLedger.createNode(
              ledgerHandle.getId
            )

            lock.writeLock().lock()
            try {
              currentOpenedLedger = ledgerHandle
            }
            finally {
              lock.writeLock().unlock()
            }

            while(
              previousOpenedLedger.getLastAddPushed !=
                previousOpenedLedger.getLastAddConfirmed
            ) {}
            if (previousOpenedLedger != null) {
              closeLedger(previousOpenedLedger)
            }

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
                                  digestType: DigestType) = {
    val metadata =
      new java.util.HashMap[String, Array[Byte]]

    val size =
      java.lang.Long.BYTES
    val time = System.currentTimeMillis()
    val buffer =
      java.nio.ByteBuffer
        .allocate(size)
        .putLong(time)
    buffer.flip()

    val bytes = if (buffer.hasArray)
      buffer.array()
    else {
      val bytes = new Array[Byte](size)
      buffer.get(bytes)
      bytes
    }

    metadata.put(LedgerHandle.KeyTime, bytes)

    bookKeeper.createLedger(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      digestType,
      bookkeeperOptions.password,
      metadata
    )
  }

  @tailrec
  private def retryToGetLedger: Either[ServerIsSlaveException, org.apache.bookkeeper.client.LedgerHandle] = {
    val openedLedger = currentOpenedLedger
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
      lock.readLock().lock()
      try {
          val ledgerHandle = retryToGetLedger
          operate(ledgerHandle)
      }
      catch {
        case throwable: Throwable =>
          throw throwable
      }
      finally {
        lock.readLock().unlock()
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
          if (currentOpenedLedger != null) {
            val openedLedger = currentOpenedLedger
            currentOpenedLedger = null
            closeLedger(openedLedger)
          }
        }
      }
    }
    catch {
      case _: java.lang.InterruptedException =>
        Thread.currentThread().interrupt()
    }
  }

}
