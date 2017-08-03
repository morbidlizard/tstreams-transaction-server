package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.CommonPrefixesOptions
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

class BookkeeperUtil(zookeeperClient: CuratorFramework,
                     replicationConfig: ReplicationConfig,
                     commonPrefixesOptions: CommonPrefixesOptions) {

  private val bookKeeper: BookKeeper = {
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

  private val commonMasterZkTreeList =
    new ZookeeperTreeListLong(
      zookeeperClient,
      commonPrefixesOptions.commonMasterZkTreeListPrefix
    )

  private val checkpointMasterZkTreeList =
    new ZookeeperTreeListLong(
      zookeeperClient,
      commonPrefixesOptions.checkpointMasterZkTreeListPrefix
    )


  private def createMaster(zKMasterElector: ZKMasterElector,
                           password: Array[Byte],
                           timeBetweenCreationOfLedgersMs: Int,
                           zookeeperTreeListLong: ZookeeperTreeListLong) = {
    val ledgersForWriting =
      new java.util.concurrent.LinkedBlockingQueue[org.apache.bookkeeper.client.LedgerHandle](10)

    val zKMasterElectorWrapper =
      new LeaderSelector(zKMasterElector)

    val commonBookkeeperMaster =
      new BookkeeperMaster(
        bookKeeper,
        zKMasterElectorWrapper,
        replicationConfig,
        zookeeperTreeListLong,
        password,
        timeBetweenCreationOfLedgersMs,
        ledgersForWriting
      )

    val commonBookkeeperCurrentLedgerAccessor =
      new BookkeeperCurrentLedgerAccessor(
        zookeeperClient,
        zKMasterElectorWrapper,
        ledgersForWriting
      )
    new BookkeeperWriteBundle(
      commonBookkeeperCurrentLedgerAccessor,
      commonBookkeeperMaster,
      timeBetweenCreationOfLedgersMs
    )
  }

  def createCommonMaster(zKMasterElector: ZKMasterElector,
                         password: Array[Byte],
                         timeBetweenCreationOfLedgersMs: Int): BookkeeperWriteBundle = {
    createMaster(
      zKMasterElector,
      password,
      timeBetweenCreationOfLedgersMs,
      commonMasterZkTreeList
    )
  }

  def createCheckpointMaster(zKMasterElector: ZKMasterElector,
                             password: Array[Byte],
                             timeBetweenCreationOfLedgersMs: Int): BookkeeperWriteBundle = {
    createMaster(
      zKMasterElector,
      password,
      timeBetweenCreationOfLedgersMs,
      checkpointMasterZkTreeList
    )
  }


  def createCommonSlave() = ???

}
