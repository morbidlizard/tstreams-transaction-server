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
import scala.concurrent.ExecutionContextExecutorService

final class BookKeeperGateway(zkClient: CuratorFramework,
                              selector: Electable,
                              replicationConfig: ReplicationConfig,
                              ledgerLogPath: String,
                              bookKeeperPathPassword: Array[Byte],
                              timeBetweenCreationOfLedgers: Int,
                              ledgerToStartToRead: Long)
  extends Runnable
{

  private val ledgersToWriteTo =
    new LinkedBlockingQueue[LedgerHandle](10)

  private val ledgersToReadFrom =
    new LinkedBlockingQueue[LedgerHandle](100)

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
    ???,
    bookKeeperPathPassword,
    timeBetweenCreationOfLedgers,
    ledgersToWriteTo
  )

  private val slave = new Slave(
    zkClient,
    bookKeeper,
    selector,
    bookKeeperPathPassword,
    timeBetweenCreationOfLedgers,
    ledgersToReadFrom

  )

  private val addNewLedgersIfMasterAndReadNewLedgersIfSlaveTask =
    new Runnable {
      override def run(): Unit = {
        while (true) {
          if (selector.hasLeadership)
            master.lead()
        }
      }
    }

  val masterTask = new Thread(
    addNewLedgersIfMasterAndReadNewLedgersIfSlaveTask,
    "bookkeeper-master-%d"
  )

  val slaveTask = new Thread(
//    slave.follow(ledgerID)
    "bookkeeper-slave-%d"
  )


  def init(): Thread = {
    masterTask.run()
    masterTask
  }

  private val lock = new ReentrantLock()
  @throws[Exception]
  def doOperationWithCurrentWriteLedger(operate: LedgerHandle => Unit): Unit = {

    @tailrec
    def retryToGetLedger: LedgerHandle = {
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
    val ledgerNumber = ledgersToWriteTo.size()
    if (ledgerNumber > 1) {
      val ledger = ledgersToWriteTo.poll()
      ledger.close()
    }
    lock.unlock()
  }

  def close(): Unit = {
    selector.stopParticipateInElection()
  }
}
