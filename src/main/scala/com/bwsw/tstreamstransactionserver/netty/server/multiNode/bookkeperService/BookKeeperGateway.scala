package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

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

final class BookKeeperGateway(rocksReader: RocksReader,
                              rocksWriter: RocksWriter,
                              zkClient: CuratorFramework,
                              selector: Electable,
                              zkTress: Array[ZookeeperTreeListLong],
                              replicationConfig: ReplicationConfig,
                              bookKeeperLedgerPassword: Array[Byte],
                              timeBetweenCreationOfLedgersMs: Int)
  extends Runnable {

  private type Timestamp = Long
  private val myZkTreeList = zkTress.head
  private val bookKeeperExecutor =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("bookkeeper-close-ledger-scheduler-%d").build()
    )
  private val bookKeeper: BookKeeper = {
    val lowLevelZkClient = zkClient.getZookeeperClient
    val configuration = new ClientConfiguration()
      .setZkServers(
        lowLevelZkClient.getCurrentConnectionString
      )
      .setZkTimeout(lowLevelZkClient.getConnectionTimeoutMs)

    configuration.setLedgerManagerFactoryClass(
      classOf[HierarchicalLedgerManagerFactory]
    )

    new BookKeeper(configuration)
  }
  private val ledgersToWriteTo =
    new LinkedBlockingQueue[org.apache.bookkeeper.client.LedgerHandle](10)
  private val master = new Master(
    bookKeeper,
    selector,
    replicationConfig,
    myZkTreeList,
    bookKeeperLedgerPassword,
    timeBetweenCreationOfLedgersMs,
    ledgersToWriteTo
  )
  private val slave = new Slave(
    bookKeeper,
    replicationConfig,
    zkTress,
    rocksReader,
    rocksWriter,
    bookKeeperLedgerPassword
  )
  private val masterTask =
    new Thread(
      master,
      "bookkeeper-master-%d"
    )
  private val bookKeeperSlaveExecutor =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("bookkeeper-slave-%d").build()
    )
  private val lock = new ReentrantReadWriteLock()

  def init(): Unit = {
    masterTask.start()
    bookKeeperSlaveExecutor.scheduleWithFixedDelay(
      slave,
      0L,
      timeBetweenCreationOfLedgersMs,
      TimeUnit.MILLISECONDS
    )
  }

  @throws[Exception]
  def doOperationWithCurrentWriteLedger(operate: org.apache.bookkeeper.client.LedgerHandle => Unit): Unit = {

    @tailrec
    def retryToGetLedger: org.apache.bookkeeper.client.LedgerHandle = {
      val openedLedger = ledgersToWriteTo.peek()
      if (openedLedger == null) {
        TimeUnit.MILLISECONDS.sleep(10)
        retryToGetLedger
      }
      else {
        openedLedger
      }
    }

    if (selector.hasLeadership) {
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
    val ledgerNumber = ledgersToWriteTo.size()
    if (ledgerNumber > 1) {
      val ledger = ledgersToWriteTo.poll()
      lock.writeLock().unlock()
      ledger.close()
    } else {
      lock.writeLock().unlock()
    }
  }

  def shutdown(): Unit = {
    masterTask.interrupt()
    bookKeeperExecutor.shutdown()
    Try {
      bookKeeperExecutor.awaitTermination(
        timeBetweenCreationOfLedgersMs * 5,
        TimeUnit.MILLISECONDS
      )
    }

    selector.stopParticipateInElection()

    bookKeeperSlaveExecutor.shutdown()
    Try {
      bookKeeperSlaveExecutor.awaitTermination(
        timeBetweenCreationOfLedgersMs * 5,
        TimeUnit.MILLISECONDS
      )
    }
  }

  bookKeeperExecutor.scheduleWithFixedDelay(
    this,
    0L,
    timeBetweenCreationOfLedgersMs,
    TimeUnit.MILLISECONDS
  )
}
