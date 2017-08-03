package com.bwsw.tstreamstransactionserver.netty.server.multiNode.common

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.exception.Throwable.InvalidSocketAddress
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.server.multiNode
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZookeeperClient
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import org.apache.curator.retry.RetryForever

class CommonServer(authenticationOpts: AuthenticationOptions,
                   zookeeperOpts: CommonOptions.ZookeeperOptions,
                   serverOpts: BootstrapOptions,
                   commonRoleOptions: CommonRoleOptions,
                   checkpointGroupRoleOptions: CheckpointGroupRoleOptions,
                   serverReplicationOpts: ServerReplicationOptions,
                   storageOpts: StorageOptions,
                   rocksStorageOpts: RocksStorageOptions,
                   commitLogOptions: CommitLogOptions,
                   packageTransmissionOpts: TransportOptions,
                   subscribersUpdateOptions: SubscriberUpdateOptions) {
  private val isShutdown = new AtomicBoolean(false)

  private def createTransactionServerExternalSocket() = {
    val externalHost = System.getenv("HOST")
    val externalPort = System.getenv("PORT0")

    SocketHostPortPair
      .fromString(s"$externalHost:$externalPort")
      .orElse(
        SocketHostPortPair.validateAndCreate(
          serverOpts.bindHost,
          serverOpts.bindPort
        )
      )
      .getOrElse {
        if (externalHost == null || externalPort == null)
          throw new InvalidSocketAddress(
            s"Socket ${serverOpts.bindHost}:${serverOpts.bindPort} is not valid for external access."
          )
        else
          throw new InvalidSocketAddress(
            s"Environment parameters 'HOST':'PORT0' " +
              s"${serverOpts.bindHost}:${serverOpts.bindPort} are not valid for a socket."
          )
      }
  }

  if (!SocketHostPortPair.isValid(serverOpts.bindHost, serverOpts.bindPort)) {
    throw new InvalidSocketAddress(
      s"Address ${serverOpts.bindHost}:${serverOpts.bindPort} is not a correct socket address pair."
    )
  }

  private val transactionServerSocketAddress =
    createTransactionServerExternalSocket()

  private val zk =
    new ZookeeperClient(
      zookeeperOpts.endpoints,
      zookeeperOpts.sessionTimeoutMs,
      zookeeperOpts.connectionTimeoutMs,
      new RetryForever(zookeeperOpts.retryDelayMs)
    )

  private val commonMasterZkTreeListPrefix =
    "/tts/common/master_tree"
  private val checkpointMasterZkTreeListPrefix =
    "/tts/cg/master_tree"

  private val replicationConfig =
    ReplicationConfig(5,3,3)


  private val commonMasterElector =
    zk.masterElector(
      transactionServerSocketAddress,
      commonRoleOptions.commonMasterPrefix,
      commonRoleOptions.commonMasterElectionPrefix
    )

  private val checkpointGroupMasterElector =
    zk.masterElector(
      transactionServerSocketAddress,
      checkpointGroupRoleOptions.checkpointGroupMasterPrefix,
      checkpointGroupRoleOptions.checkpointGroupMasterElectionPrefix
    )

//  private val multiNodeCommitLogService =
//    new multiNode.commitLogService.CommitLogService(
//      rocksStorage.getRocksStorage
//    )

}
