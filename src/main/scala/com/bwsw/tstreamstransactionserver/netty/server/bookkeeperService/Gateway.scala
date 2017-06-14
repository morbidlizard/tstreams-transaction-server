package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.locks.ReentrantLock

import org.apache.bookkeeper.client.{BookKeeper, LedgerHandle}
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

final class Gateway(zkClient: CuratorFramework,
              masterSelector: ServerRole,
              ledgerLogPath: String,
              bookKeeperPathPassword: Array[Byte],
              timeBetweenCreationOfLedgers: Int
             )
  extends Runnable {
  private val openedLedgers =
    new ArrayBlockingQueue[LedgerHandle](5)

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
    ledgerLogPath,
    bookKeeperPathPassword,
    timeBetweenCreationOfLedgers,
    openedLedgers
  )


  private val lock = new ReentrantLock()

  def currentLedgerHandle: Option[LedgerHandle] = {
    lock.lock()
    val ledger = Option(openedLedgers.peek())
    lock.unlock()
    ledger
  }

  override def run(): Unit = {
    lock.lock()
    val ledgerNumber = openedLedgers.size()
    if (ledgerNumber > 1)
      openedLedgers.poll().close()
    lock.unlock()
  }
}
