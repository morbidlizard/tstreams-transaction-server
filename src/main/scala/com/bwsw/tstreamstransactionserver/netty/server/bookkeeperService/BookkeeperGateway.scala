package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

import java.util.concurrent._
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid
import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerIsSlaveException
import org.apache.bookkeeper.client.{BookKeeper, LedgerHandle}
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

import scala.concurrent.ExecutionContextExecutorService

final class BookkeeperGateway(zkClient: CuratorFramework,
                              masterSelector: ServerRole,
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

  def openedLedgersNumber: Int =
    openedLedgers.size()

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
    masterSelector,
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
    masterSelector,
    ledgerLogPath,
    bookKeeperPathPassword,
    ledgersToReadFrom
  )

  private val addNewLedgersIfMasterTask = new Callable[Unit] {
    override def call(): Unit = {
      val entryId = EntryId(-1)
      while (true) {
        if (masterSelector.hasLeadership)
          master.lead(entryId)
        else
          slave.follow(entryId)
      }
    }
  }

  private lazy val bookieContext: ExecutionContextExecutorService =
    ExecutionContextGrid("bookkeeper-master-%d").getContext

  def init(): Future[Unit] = bookieContext.submit(addNewLedgersIfMasterTask)

  private val lock = new ReentrantLock()
  def currentLedgerHandle: Either[ServerIsSlaveException, Option[LedgerHandle]] = {
    if (masterSelector.hasLeadership) {
      lock.lock()
      val ledgerHandle = Option(openedLedgers.peek())
      lock.unlock()
      Right(ledgerHandle)
    } else {
      Left(new ServerIsSlaveException)
    }
  }

  override def run(): Unit = {
    lock.lock()
    val ledgerNumber = openedLedgers.size()
    if (ledgerNumber > 1) {
      val ledger = openedLedgers.poll()
      ledger.close()
      ledgersToReadFrom.add(ledger)
    }
    lock.unlock()
  }
}
