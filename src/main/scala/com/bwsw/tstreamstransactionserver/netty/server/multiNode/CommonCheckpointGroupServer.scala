package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.exception.Throwable.InvalidSocketAddress
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.ReplicationConfig
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.storage.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.{OpenedTransactionNotifier, SubscriberNotifier, SubscribersObserver}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZookeeperClient
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.CommonPrefixesOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.{ExecutionContextGrid, SinglePoolExecutionContextGrid}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.ServerSocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelOption, EventLoopGroup}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever

class CommonCheckpointGroupServer(authenticationOpts: AuthenticationOptions,
                                  zookeeperOpts: CommonOptions.ZookeeperOptions,
                                  serverOpts: BootstrapOptions,
                                  commonRoleOptions: CommonRoleOptions,
                                  checkpointGroupRoleOptions: CheckpointGroupRoleOptions,
                                  commonPrefixesOptions: CommonPrefixesOptions,
                                  replicationConfig: ReplicationConfig,
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


  protected val rocksStorage: MultiAndSingleNodeRockStorage =
    new MultiAndSingleNodeRockStorage(
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

  private val rocksReader = new RocksReader(
    rocksStorage,
    transactionDataService
  )

  private val multiNodeCommitLogService =
    new CommitLogService(
      rocksStorage.getRocksStorage
    )

  private val transactionServer = new TransactionServer(
    zkStreamRepository,
    rocksWriter,
    rocksReader
  )

  private val bookkeeperToRocksWriter =
    new CommonCheckpointGroupBookkeeperWriter(
      zk.client,
      replicationConfig,
      commonPrefixesOptions
    )

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


  private val commonMaster = bookkeeperToRocksWriter
    .createCommonMaster(
      commonMasterElector,
      "test".getBytes(),
      commitLogOptions.closeDelayMs
    )

  private val checkpointMaster = bookkeeperToRocksWriter
    .createCheckpointMaster(
      checkpointGroupMasterElector,
      "test".getBytes(),
      commitLogOptions.closeDelayMs
    )

  private val slave = bookkeeperToRocksWriter
    .createSlave(
      multiNodeCommitLogService,
      rocksWriter,
      "test".getBytes(),
      commitLogOptions.closeDelayMs
    )


  val (bossGroup: EventLoopGroup, workerGroup: EventLoopGroup) = {
    if (Epoll.isAvailable)
      (new EpollEventLoopGroup(1), new EpollEventLoopGroup())
    else
      (new NioEventLoopGroup(1), new NioEventLoopGroup())
  }

  private def determineChannelType(): Class[_ <: ServerSocketChannel] =
    workerGroup match {
      case _: EpollEventLoopGroup => classOf[EpollServerSocketChannel]
      case _: NioEventLoopGroup => classOf[NioServerSocketChannel]
      case group => throw new IllegalArgumentException(
        s"Can't determine channel type for group '$group'."
      )
    }


  private val orderedExecutionPool =
    new OrderedExecutionContextPool(serverOpts.openOperationsPoolSize)


  private val curatorSubscriberClient =
    subscribersUpdateOptions.monitoringZkEndpoints.map {
      monitoringZkEndpoints =>
        if (monitoringZkEndpoints == zookeeperOpts.endpoints) {
          zk
        }
        else {
          new ZookeeperClient(
            monitoringZkEndpoints,
            zookeeperOpts.sessionTimeoutMs,
            zookeeperOpts.connectionTimeoutMs,
            new RetryForever(zookeeperOpts.retryDelayMs)
          )
        }
    }.getOrElse(zk)

  private val openedTransactionNotifier =
    new OpenedTransactionNotifier(
      new SubscribersObserver(
        curatorSubscriberClient.client,
        zkStreamRepository,
        subscribersUpdateOptions.updatePeriodMs
      ),
      new SubscriberNotifier
    )

  private val executionContext =
    new ServerExecutionContextGrids(
      rocksStorageOpts.readThreadPool,
      rocksStorageOpts.writeThreadPool
    )

  private val commitLogContext: SinglePoolExecutionContextGrid =
    ExecutionContextGrid("CommitLogExecutionContextGrid-%d")


  private val requestRouter =
    new CommonCheckpointGroupHandlerRouter(
      transactionServer,
      commonMaster.bookkeeperMaster,
      checkpointMaster.bookkeeperMaster,
      multiNodeCommitLogService,
      packageTransmissionOpts,
      authenticationOpts,
      orderedExecutionPool,
      openedTransactionNotifier,
      checkpointGroupRoleOptions,
      executionContext,
      commitLogContext.getContext
    )

  def start(function: => Unit = ()): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(determineChannelType())
        .handler(new LoggingHandler(LogLevel.DEBUG))
        .childHandler(
          new ServerInitializer(requestRouter)
        )
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)

      val binding = b
        .bind(serverOpts.bindHost, serverOpts.bindPort)
        .sync()

      commonMasterElector.start()
      checkpointGroupMasterElector.start()

      commonMaster.start()
      checkpointMaster.start()
      slave.start()

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
      if (commonMasterElector != null)
        commonMasterElector.stop()

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

      if (zk != null && curatorSubscriberClient != null) {
        if (zk == curatorSubscriberClient) {
          zk.close()
        }
        else {
          zk.close()
          curatorSubscriberClient.close()
        }
      }

      if (slave != null) {
        slave.stop()
      }

      if (commonMaster != null) {
        commonMaster.stop()
      }

      if (checkpointMaster != null) {
        checkpointMaster.stop()
      }

      if (orderedExecutionPool != null) {
        orderedExecutionPool.close()
      }

      if (commitLogContext != null) {
        commitLogContext.stopAccessNewTasks()
        commitLogContext.awaitAllCurrentTasksAreCompleted()
      }

      if (executionContext != null) {
        executionContext
          .stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
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
