package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerIsSlaveException
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
                       timeBetweenCreationOfLedgers: Int)
  extends Runnable {

  private val lock = new ReentrantReadWriteLock()

  private val openedLedgers =
    new java.util.concurrent.LinkedBlockingQueue[org.apache.bookkeeper.client.LedgerHandle](10)

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
              replicationConfig.ensembleNumber,
              replicationConfig.writeQuorumNumber,
              replicationConfig.ackQuorumNumber,
              BookKeeper.DigestType.MAC
            )
          }.map { ledgerHandle =>

            lock.writeLock().lock()
            val ledgerNumber = openedLedgers.size()
            if (ledgerNumber > 1) {
              val ledger = openedLedgers.poll()
              lock.writeLock().unlock()
              ledger.close()
            } else {
              lock.writeLock().unlock()
            }

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
  def doOperationWithCurrentWriteLedger(operate: Either[ServerIsSlaveException, org.apache.bookkeeper.client.LedgerHandle] => Unit): Unit = {
    if (master.hasLeadership) {
      scala.util.Try {
        lock.readLock().lock()
        val currentLedgerHandle = retryToGetLedger
        operate(currentLedgerHandle)
      } match {
        case scala.util.Success(_) =>
          lock.readLock().unlock()
        case scala.util.Failure(throwable) =>
          lock.readLock().unlock()
          throw throwable
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
          lock.writeLock().lock()
          val ledgers =
            new util.LinkedList[org.apache.bookkeeper.client.LedgerHandle]()
          openedLedgers.drainTo(ledgers)
          ledgers.forEach(_.close())
          lock.writeLock().unlock()
        }
      }
    } catch {
      case _: java.lang.InterruptedException =>
        Thread.currentThread().interrupt()
    }
  }

}
