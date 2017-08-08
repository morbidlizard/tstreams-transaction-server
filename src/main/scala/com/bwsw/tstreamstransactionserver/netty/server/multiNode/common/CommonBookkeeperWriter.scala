package com.bwsw.tstreamstransactionserver.netty.server.multiNode.common

import com.bwsw.tstreamstransactionserver.netty.server.RocksWriter
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.CommonPrefixesOptions
import org.apache.curator.framework.CuratorFramework

class CommonBookkeeperWriter(zookeeperClient: CuratorFramework,
                             replicationConfig: ReplicationConfig,
                             commonPrefixesOptions: CommonPrefixesOptions)
  extends BookkeeperWriter(
    zookeeperClient,
    replicationConfig) {

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

  private val zkTreesList =
    Array(commonMasterZkTreeList, checkpointMasterZkTreeList)


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

  def createCommonSlave(commitLogService: CommitLogService,
                        rocksWriter: RocksWriter,
                        password: Array[Byte],
                        timeBetweenCreationOfLedgersMs: Int): BookkeeperSlaveBundle = {
    val bookkeeperSlave =
      new BookkeeperSlave(
        bookKeeper,
        replicationConfig,
        zkTreesList,
        commitLogService,
        rocksWriter,
        password
      )
    new BookkeeperSlaveBundle(bookkeeperSlave, timeBetweenCreationOfLedgersMs)
  }
}
