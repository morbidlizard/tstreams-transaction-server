package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

import java.util
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid
import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerIsSlaveException
import org.apache.bookkeeper.client.{BookKeeper, LedgerHandle}
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContextExecutorService

final class BookkeeperGateway(zkClient: CuratorFramework,
                              selector: Electable,
                              replicationConfig: ReplicationConfig,
                              ledgerLogPath: String,
                              bookKeeperPathPassword: Array[Byte],
                              timeBetweenCreationOfLedgers: Int)
  extends Runnable
{

  private val openedLedgers =
    new LinkedBlockingQueue[LedgerHandle](10)

  private val ledgersToReadFrom =
    new LinkedBlockingQueue[LedgerHandle](100)

  private val ledgersToReadBuffer =
    new util.ArrayList[LedgerHandle]()


  def openedLedgersNumber: Int =
    openedLedgers.size()

  def getClosedLedgers: util.Collection[LedgerHandle] = {
    val ledgers = new util.LinkedList[LedgerHandle]()
    ledgersToReadFrom.drainTo(ledgers)
    ledgers
  }

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


  private val master = new Master(
    zkClient,
    bookKeeper,
    selector,
    replicationConfig,
    ledgerLogPath,
    bookKeeperPathPassword,
    timeBetweenCreationOfLedgers,
    openedLedgers,
    ledgersToReadFrom
  )

  private val slave = new Slave(
    zkClient,
    bookKeeper,
    selector,
    ledgerLogPath,
    bookKeeperPathPassword,
    ledgersToReadFrom
  )

  private val addNewLedgersIfMasterAndReadNewLedgersIfSlaveTask = new Callable[Unit] {
    override def call(): Unit = {
      var ledgerID = LedgerID(-1)
      while (true) {
        if (selector.hasLeadership)
          ledgerID = master.lead(ledgerID)
        else
          ledgerID = slave.follow(ledgerID)
      }
    }
  }

  private lazy val bookieContext: ExecutionContextExecutorService =
    ExecutionContextGrid("bookkeeper-master-%d").getContext

  def init(): Future[Unit] = bookieContext.submit(
    addNewLedgersIfMasterAndReadNewLedgersIfSlaveTask
  )

  private val lock = new ReentrantLock()
  @throws[Exception]
  def doOperationWithCurrentWriteLedger(operate: LedgerHandle => Unit): Unit = {

    @tailrec
    def retryToGetLedger: LedgerHandle = {
      val openedLedger = openedLedgers.peek()
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
        lock.lock()
        operate(retryToGetLedger)
      } match {
        case scala.util.Success(_) =>
          lock.unlock()
        case scala.util.Failure(throwable) =>
          lock.unlock()
          throw throwable
      }
    } else {
      throw new ServerIsSlaveException
    }
  }

  override def run(): Unit = {
    lock.lock()
    val ledgerNumber = openedLedgers.size()
    if (ledgerNumber > 1) {
      val ledger = openedLedgers.poll()
      ledger.close()
      if (master.areAllClosedLedgersGotten) {
        val buffer = new util.LinkedList[LedgerHandle]()
        ledgersToReadBuffer.removeAll(buffer)
        buffer.add(ledger)

        ledgersToReadFrom.addAll(buffer)
      } else {
        ledgersToReadBuffer.add(ledger)
      }
    }
    lock.unlock()
  }

  def close(): Unit = {
    selector.stopParticipateInElection()
    master.close()
  }
}
