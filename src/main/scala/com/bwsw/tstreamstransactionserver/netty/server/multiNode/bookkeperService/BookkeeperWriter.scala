package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

abstract class BookkeeperWriter(zookeeperClient: CuratorFramework,
                                replicationConfig: ReplicationConfig) {

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
                                   password: Array[Byte],
                                   timeBetweenCreationOfLedgersMs: Int,
                                   zookeeperTreeListLong: ZookeeperTreeListLong): BookkeeperWriteBundle = {
    val zKMasterElectorWrapper =
      new LeaderSelector(zKMasterElector)

    val commonBookkeeperMaster =
      new BookkeeperMaster(
        bookKeeper,
        zKMasterElectorWrapper,
        replicationConfig,
        zookeeperTreeListLong,
        password,
        timeBetweenCreationOfLedgersMs
      )

    new BookkeeperWriteBundle(
      commonBookkeeperMaster,
      timeBetweenCreationOfLedgersMs
    )
  }
}
