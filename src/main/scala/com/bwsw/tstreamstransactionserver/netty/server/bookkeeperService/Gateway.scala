package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

import java.util.concurrent.ArrayBlockingQueue

import org.apache.bookkeeper.client.{BookKeeper, LedgerHandle}
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

class Gateway(zkClient: CuratorFramework,
              masterSelector: ServerRole,
              ledgerLogPath: String,
              bookKeeperPathPassword: Array[Byte],
              timeBetweenCreationOfLedgers: Int
             )
  extends Runnable
{
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

  @volatile private var currentLedgerHandle: Option[LedgerHandle] = None
  override def run(): Unit = {
    val ledgerHandle = Option(openedLedgers.poll())
    var ledgerOpenedNumber = openedLedgers.size()
    if (ledgerOpenedNumber > 1) {
      ledgerOpenedNumber = ledgerOpenedNumber - 1
      currentLedgerHandle = ledgerHandle
    }
    else if (ledgerOpenedNumber > 0) {
      ledgerHandle.foreach(handle => handle.close())
    }
  }
}
