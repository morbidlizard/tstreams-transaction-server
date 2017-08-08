package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{BookkeeperMasterBundle, BookkeeperWriter}
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CheckpointGroupPrefixesOptions}
import org.apache.curator.framework.CuratorFramework

class CheckpointGroupBookkeeperWriter(zookeeperClient: CuratorFramework,
                                      bookkeeperOptions: BookkeeperOptions,
                                      checkpointGroupPrefixesOptions: CheckpointGroupPrefixesOptions)
  extends BookkeeperWriter(
    zookeeperClient,
    bookkeeperOptions
  ) {

  private val checkpointMasterZkTreeList =
    new ZookeeperTreeListLong(
      zookeeperClient,
      checkpointGroupPrefixesOptions.checkpointMasterZkTreeListPrefix
    )

  def createCheckpointMaster(zKMasterElector: ZKMasterElector,
                             timeBetweenCreationOfLedgersMs: Int): BookkeeperMasterBundle = {
    createMaster(
      zKMasterElector,
      timeBetweenCreationOfLedgersMs,
      checkpointMasterZkTreeList
    )
  }
}
