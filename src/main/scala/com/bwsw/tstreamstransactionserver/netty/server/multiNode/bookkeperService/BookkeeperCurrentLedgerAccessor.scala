package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.util
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerIsSlaveException
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

import scala.annotation.tailrec
import scala.util.Try

final class BookkeeperCurrentLedgerAccessor(zkClient: CuratorFramework,
                                            leaderSelector: LeaderSelectorInterface,
                                            ledgersToWrite: BlockingQueue[org.apache.bookkeeper.client.LedgerHandle])
  extends Runnable {
//
//  private val ledgersToWriteTo =
//    new LinkedBlockingQueue[org.apache.bookkeeper.client.LedgerHandle](10)
//  private val master = new Master(
//    bookKeeper,
//    selector,
//    replicationConfig,
//    myZkTreeList,
//    bookKeeperLedgerPassword,
//    timeBetweenCreationOfLedgersMs,
//    ledgersToWriteTo
//  )

//  private val masterTask =
//    new Thread(
//      master,
//      "bookkeeper-master-%d"
//    )

  private val lock = new ReentrantReadWriteLock()

  @tailrec
  private def retryToGetLedger: org.apache.bookkeeper.client.LedgerHandle = {
    val openedLedger = ledgersToWrite.peek()
    if (openedLedger == null) {
      if (leaderSelector.hasLeadership) {
        TimeUnit.MILLISECONDS.sleep(10)
        retryToGetLedger
      }
      else {
        throw new ServerIsSlaveException
      }
    }
    else {
      openedLedger
    }
  }

  @throws[Exception]
  def doOperationWithCurrentWriteLedger(operate: org.apache.bookkeeper.client.LedgerHandle => Unit): Unit = {

    if (leaderSelector.hasLeadership) {
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
      throw new ServerIsSlaveException
    }
  }

  override def run(): Unit = {
    lock.writeLock().lock()
    val ledgerNumber = ledgersToWrite.size()
    if (ledgerNumber > 1) {
      val ledger = ledgersToWrite.poll()
      lock.writeLock().unlock()
      ledger.close()
    }
    else {
      if (!leaderSelector.hasLeadership) {
        val ledgers =
          new util.LinkedList[org.apache.bookkeeper.client.LedgerHandle]()
        ledgersToWrite.drainTo(ledgers)
        ledgers.forEach(_.close())
      }
      lock.writeLock().unlock()
    }
  }


//  private val bookKeeperExecutor =
//    Executors.newSingleThreadScheduledExecutor(
//      new ThreadFactoryBuilder().setNameFormat("bookkeeper-close-ledger-scheduler-%d").build()
//    )
//  private val bookKeeperSlaveExecutor =
//    Executors.newSingleThreadScheduledExecutor(
//      new ThreadFactoryBuilder().setNameFormat("bookkeeper-slave-%d").build()
//    )
//
//  bookKeeperExecutor.scheduleWithFixedDelay(
//    this,
//    0L,
//    timeBetweenCreationOfLedgersMs,
//    TimeUnit.MILLISECONDS
//  )

  def shutdown(): Unit = {
//    masterTask.interrupt()
    //    bookKeeperExecutor.shutdown()
    //    Try {
    //      bookKeeperExecutor.awaitTermination(
    //        0L,
    //        TimeUnit.NANOSECONDS
    //      )
    //    }
    //
    //    bookKeeperSlaveExecutor.shutdown()
    //    Try {
    //      bookKeeperSlaveExecutor.awaitTermination(
    //        0L,
    //        TimeUnit.NANOSECONDS
    //      )
    //    }
    //  }
  }
}
