package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{BookkeeperWriter, BookkeeperMasterBundle, ReplicationConfig}
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.CheckpointGroupPrefixesOptions
import org.apache.curator.framework.CuratorFramework

class CheckpointGroupBookkeeperWriter(zookeeperClient: CuratorFramework,
                                      replicationConfig: ReplicationConfig,
                                      checkpointGroupPrefixesOptions: CheckpointGroupPrefixesOptions)
  extends BookkeeperWriter(
    zookeeperClient,
    replicationConfig
  ) {

  private val checkpointMasterZkTreeList =
    new ZookeeperTreeListLong(
      zookeeperClient,
      checkpointGroupPrefixesOptions.checkpointMasterZkTreeListPrefix
    )

  def createCheckpointMaster(zKMasterElector: ZKMasterElector,
                             password: Array[Byte],
                             timeBetweenCreationOfLedgersMs: Int): BookkeeperMasterBundle = {
    createMaster(
      zKMasterElector,
      password,
      timeBetweenCreationOfLedgersMs,
      checkpointMasterZkTreeList
    )
  }
}
