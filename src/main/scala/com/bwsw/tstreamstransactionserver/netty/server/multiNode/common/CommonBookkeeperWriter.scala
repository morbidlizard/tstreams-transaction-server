package com.bwsw.tstreamstransactionserver.netty.server.multiNode.common

import com.bwsw.tstreamstransactionserver.netty.server.RocksWriter
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CommonPrefixesOptions}
import org.apache.curator.framework.CuratorFramework

class CommonBookkeeperWriter(zookeeperClient: CuratorFramework,
                             bookkeeperOptions: BookkeeperOptions,
                             commonPrefixesOptions: CommonPrefixesOptions)
  extends BookkeeperWriter(
    zookeeperClient,
    bookkeeperOptions) {

  private val commonMasterZkTreeList =
    new ZookeeperTreeListLong(
      zookeeperClient,
      commonPrefixesOptions.commonMasterZkTreeListPrefix
    )

  private val checkpointMasterZkTreeList =
    new ZookeeperTreeListLong(
      zookeeperClient,
      commonPrefixesOptions.checkpointGroupPrefixesOptions.checkpointMasterZkTreeListPrefix
    )

  private val zkTreesList =
    Array(commonMasterZkTreeList, checkpointMasterZkTreeList)


  def createCommonMaster(zKMasterElector: ZKMasterElector,
                         timeBetweenCreationOfLedgersMs: Int): BookkeeperMasterBundle = {
    createMaster(
      zKMasterElector,
      timeBetweenCreationOfLedgersMs,
      commonMasterZkTreeList
    )
  }

  def createSlave(commitLogService: CommitLogService,
                  rocksWriter: RocksWriter,
                  timeBetweenCreationOfLedgersMs: Int): BookkeeperSlaveBundle = {
    val bookkeeperSlave =
      new BookkeeperSlave(
        bookKeeper,
        bookkeeperOptions,
        zkTreesList,
        commitLogService,
        rocksWriter,
      )
    new BookkeeperSlaveBundle(bookkeeperSlave, timeBetweenCreationOfLedgersMs)
  }
}
