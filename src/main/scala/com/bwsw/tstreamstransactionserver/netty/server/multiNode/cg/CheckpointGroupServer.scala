package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.{ExecutionContextGrid, SinglePoolExecutionContextGrid}
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.netty.server.storage.MultiNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZookeeperClient
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CheckpointGroupPrefixesOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelOption, EventLoopGroup}
import io.netty.channel.socket.ServerSocketChannel
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever

class CheckpointGroupServer(authenticationOpts: AuthenticationOptions,
                            zookeeperOpts: CommonOptions.ZookeeperOptions,
                            serverOpts: BootstrapOptions,
                            checkpointGroupRoleOptions: CheckpointGroupRoleOptions,
                            checkpointGroupPrefixesOptions: CheckpointGroupPrefixesOptions,
                            bookkeeperOptions: BookkeeperOptions,
                            storageOpts: StorageOptions,
                            rocksStorageOpts: RocksStorageOptions,
                            commitLogOptions: CommitLogOptions,
                            packageTransmissionOpts: TransportOptions) {
  private val isShutdown = new AtomicBoolean(false)

  private val transactionServerSocketAddress =
    Util.createTransactionServerExternalSocket(
      serverOpts.bindHost,
      serverOpts.bindPort
    )

  private val zk =
    new ZookeeperClient(
      zookeeperOpts.endpoints,
      zookeeperOpts.sessionTimeoutMs,
      zookeeperOpts.connectionTimeoutMs,
      new RetryForever(zookeeperOpts.retryDelayMs)
    )

  protected val rocksStorage: MultiNodeRockStorage =
    new MultiNodeRockStorage(
      storageOpts,
      rocksStorageOpts
    )

  private val zkStreamRepository: ZookeeperStreamRepository =
    zk.streamRepository(s"${storageOpts.streamZookeeperDirectory}")

  protected val transactionDataService: TransactionDataService =
    new TransactionDataService(
      storageOpts,
      rocksStorageOpts,
      zkStreamRepository
    )

  protected lazy val rocksWriter: RocksWriter = new RocksWriter(
    rocksStorage,
    transactionDataService
  )

  private val bookkeeperToRocksWriter =
    new CheckpointGroupBookkeeperWriter(
      zk.client,
      bookkeeperOptions,
      checkpointGroupPrefixesOptions
    )

  private val checkpointGroupMasterElector =
    zk.masterElector(
      transactionServerSocketAddress,
      checkpointGroupRoleOptions.checkpointGroupMasterPrefix,
      checkpointGroupRoleOptions.checkpointGroupMasterElectionPrefix
    )

  private val checkpointMaster = bookkeeperToRocksWriter
    .createCheckpointMaster(
      checkpointGroupMasterElector,
      commitLogOptions.closeDelayMs
    )


  private val (
    bossGroup: EventLoopGroup,
    workerGroup: EventLoopGroup,
    channelType: Class[ServerSocketChannel]
    ) = Util.getBossGroupAndWorkerGroupAndChannel

  private val commitLogContext: SinglePoolExecutionContextGrid =
    ExecutionContextGrid("CommitLogExecutionContextGrid-%d")

  private val requestRouter =
    new CheckpointGroupHandlerRouter(
      checkpointMaster.bookkeeperMaster,
      commitLogContext.getContext,
      packageTransmissionOpts,
      authenticationOpts
    )

  def start(function: => Unit = ()): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(channelType)
        .handler(new LoggingHandler(LogLevel.DEBUG))
        .childHandler(
          new ServerInitializer(requestRouter)
        )
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)

      val binding = b
        .bind(serverOpts.bindHost, serverOpts.bindPort)
        .sync()

      checkpointGroupMasterElector.start()

      checkpointMaster.start()

      val channel = binding.channel().closeFuture()
      function
      channel.sync()
    } finally {
      shutdown()
    }
  }

  def shutdown(): Unit = {
    val isNotShutdown =
      isShutdown.compareAndSet(false, true)

    if (isNotShutdown) {
      if (checkpointGroupMasterElector != null)
        checkpointGroupMasterElector.stop()

      if (bossGroup != null) {
        scala.util.Try {
          bossGroup.shutdownGracefully(
            0L,
            0L,
            TimeUnit.NANOSECONDS
          ).cancel(true)
        }
      }
      if (workerGroup != null) {
        scala.util.Try {
          workerGroup.shutdownGracefully(
            0L,
            0L,
            TimeUnit.NANOSECONDS
          ).cancel(true)
        }
      }

      if (zk != null) {
        zk.close()
      }

      if (checkpointMaster != null) {
        checkpointMaster.stop()
      }


      if (commitLogContext != null) {
        commitLogContext.stopAccessNewTasks()
        commitLogContext.awaitAllCurrentTasksAreCompleted()
      }

      if (rocksStorage != null) {
        rocksStorage.getRocksStorage.closeDatabases()
      }

      if (transactionDataService != null) {
        transactionDataService.closeTransactionDataDatabases()
      }
    }
  }

}
