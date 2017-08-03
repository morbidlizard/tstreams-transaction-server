package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

object BookkeeperSupportUnit {
  def apply(zkClient: CuratorFramework,
            zkTress: Array[ZookeeperTreeListLong],
            replicationConfig: ReplicationConfig): BookkeeperSupportUnit = {

    val bookKeeper: BookKeeper = {
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
    BookkeeperSupportUnit(bookKeeper, zkTress, replicationConfig)
  }
}


case class BookkeeperSupportUnit(bookKeeper: BookKeeper,
                                 zkTress: Array[ZookeeperTreeListLong],
                                 replicationConfig: ReplicationConfig) {
  require(zkTress.nonEmpty)
}
