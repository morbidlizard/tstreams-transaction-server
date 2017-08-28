package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.LongZookeeperTreeList
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
    new LongZookeeperTreeList(
      zookeeperClient,
      checkpointGroupPrefixesOptions.checkpointMasterZkTreeListPrefix
    )

  override def getLastConstructedLedger: Long = {
    checkpointMasterZkTreeList
      .lastEntityID
      .getOrElse(-1L)
  }

  def createCheckpointMaster(zKMasterElector: ZKMasterElector): BookkeeperMasterBundle = {
    createMaster(
      zKMasterElector,
      checkpointGroupPrefixesOptions.timeBetweenCreationOfLedgersMs,
      checkpointMasterZkTreeList
    )
  }

}
