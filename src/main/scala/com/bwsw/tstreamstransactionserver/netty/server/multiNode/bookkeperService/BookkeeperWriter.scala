package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

abstract class BookkeeperWriter(zookeeperClient: CuratorFramework,
                                bookkeeperOptions: BookkeeperOptions) {

  protected final val bookKeeper: BookKeeper = {
    val lowLevelZkClient = zookeeperClient.getZookeeperClient
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

  protected final def createMaster(zKMasterElector: ZKMasterElector,
                                   timeBetweenCreationOfLedgersMs: Int,
                                   zookeeperTreeListLong: ZookeeperTreeListLong): BookkeeperMasterBundle = {
    val zKMasterElectorWrapper =
      new LeaderSelector(zKMasterElector)

    val commonBookkeeperMaster =
      new BookkeeperMaster(
        bookKeeper,
        zKMasterElectorWrapper,
        bookkeeperOptions,
        zookeeperTreeListLong,
        timeBetweenCreationOfLedgersMs
      )

    new BookkeeperMasterBundle(
      commonBookkeeperMaster,
      timeBetweenCreationOfLedgersMs
    )
  }
}
