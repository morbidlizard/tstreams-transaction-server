/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.server.singleNode

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, PriorityBlockingQueue, TimeUnit}

import com.bwsw.commitlog.filesystem.{CommitLogCatalogue, CommitLogFile, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.exception.Throwable.InvalidSocketAddress
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService._
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandlerRouter
import com.bwsw.tstreamstransactionserver.netty.server.storage.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.{OpenedTransactionNotifier, SubscriberNotifier, SubscribersObserver}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZookeeperClient
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.ServerSocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelOption, EventLoopGroup, SimpleChannelInboundHandler}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever
import org.slf4j.{Logger, LoggerFactory}


class SingleNodeServer(authenticationOpts: AuthenticationOptions,
                       zookeeperOpts: CommonOptions.ZookeeperOptions,
                       serverOpts: BootstrapOptions,
                       serverRoleOptions: ServerRoleOptions,
                       serverReplicationOpts: ServerReplicationOptions,
                       storageOpts: StorageOptions,
                       rocksStorageOpts: RocksStorageOptions,
                       commitLogOptions: CommitLogOptions,
                       packageTransmissionOpts: TransportOptions,
                       subscribersUpdateOptions: SubscriberUpdateOptions) {

//  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
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

  if (!SocketHostPortPair.isValid(serverOpts.bindHost, serverOpts.bindPort))
  {
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

  private val transactionServer = new TransactionServer(
    authenticationOpts,
    zkStreamRepository,
    rocksWriter,
    rocksReader
  )

  private val rocksDBCommitLog = new RocksDbConnection(
    rocksStorageOpts,
    s"${storageOpts.path}${java.io.File.separatorChar}${storageOpts.commitLogRocksDirectory}",
    commitLogOptions.expungeDelaySec
  )

  private val commitLogQueue = {
    val queue = new CommitLogQueueBootstrap(
      30,
      new CommitLogCatalogue(storageOpts.path + java.io.File.separatorChar + storageOpts.commitLogRawDirectory),
      transactionServer
    )
    val priorityQueue = queue.fillQueue()
    priorityQueue
  }

  /**
    * this variable is public for testing purposes only
    */
  val berkeleyWriter = new CommitLogToRocksWriter(
    rocksDBCommitLog,
    commitLogQueue,
    rocksWriter,
    commitLogOptions.incompleteReadPolicy
  )

  private val fileIDGenerator =
    zk.idGenerator(commitLogOptions.zkFileIdGeneratorPath)


  val scheduledCommitLog = new ScheduledCommitLog(
    commitLogQueue,
    storageOpts,
    commitLogOptions,
    fileIDGenerator
  )

  private val rocksWriterExecutor =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("RocksWriter-%d").build()
    )

  private val commitLogCloseExecutor =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("CommitLogClose-%d").build()
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

  private val requestHandlerRouter: RequestHandlerRouter =
    new RequestHandlerRouter(
      transactionServer,
      scheduledCommitLog,
      packageTransmissionOpts,
      authenticationOpts,
      orderedExecutionPool,
      openedTransactionNotifier,
      serverRoleOptions
    )

  private val commonMasterElector =
    zk.masterElector(
      transactionServerSocketAddress,
      zookeeperOpts.prefix,
      serverRoleOptions.commonMasterElectionPrefix
    )

  private val checkpointGroupMasterElector =
    zk.masterElector(
      transactionServerSocketAddress,
      serverRoleOptions.checkpointGroupMasterPrefix,
      serverRoleOptions.checkpointGroupMasterElectionPrefix
    )


  private val executionContext =
    new ServerExecutionContextGrids(
      rocksStorageOpts.readThreadPool,
      rocksStorageOpts.writeThreadPool
    )

  def start(function: => Unit = ()): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(determineChannelType())
        .handler(new LoggingHandler(LogLevel.DEBUG))
        .childHandler(
          new ServerInitializer(
            requestHandlerRouter,
            executionContext
          )
        )
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)

      val binding = b
        .bind(serverOpts.bindHost, serverOpts.bindPort)
        .sync()

      commitLogCloseExecutor.scheduleWithFixedDelay(
        scheduledCommitLog,
        commitLogOptions.closeDelayMs,
        commitLogOptions.closeDelayMs,
        java.util.concurrent.TimeUnit.MILLISECONDS
      )
      rocksWriterExecutor.scheduleWithFixedDelay(
        berkeleyWriter,
        0L,
        10L,
        java.util.concurrent.TimeUnit.MILLISECONDS
      )

      commonMasterElector.start()
      checkpointGroupMasterElector.start()

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

      if (rocksWriterExecutor != null) {
        rocksWriterExecutor.shutdown()
        rocksWriterExecutor.awaitTermination(
          commitLogOptions.closeDelayMs * 5,
          TimeUnit.MILLISECONDS
        )
      }

      if (scheduledCommitLog != null)
        scheduledCommitLog.closeWithoutCreationNewFile()

      if (commitLogCloseExecutor != null) {
        commitLogCloseExecutor.shutdown()
        commitLogCloseExecutor.awaitTermination(
          commitLogOptions.closeDelayMs * 5,
          TimeUnit.MILLISECONDS
        )
      }

      if (berkeleyWriter != null) {
        berkeleyWriter.run()
        berkeleyWriter.closeRocksDB()
      }

      if (orderedExecutionPool != null) {
        orderedExecutionPool.close()
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

class CommitLogQueueBootstrap(queueSize: Int, commitLogCatalogue: CommitLogCatalogue, transactionServer: TransactionServer) {
  def fillQueue(): PriorityBlockingQueue[CommitLogStorage] = {
    val allFiles = commitLogCatalogue.listAllFilesAndTheirIDs().toMap


    val berkeleyProcessedFileIDMax = transactionServer.getLastProcessedCommitLogFileID
    val (allFilesIDsToProcess, allFilesToDelete: Map[Long, CommitLogFile]) =
      if (berkeleyProcessedFileIDMax > -1)
        (allFiles.filterKeys(_ > berkeleyProcessedFileIDMax), allFiles.filterKeys(_ <= berkeleyProcessedFileIDMax))
      else
        (allFiles, collection.immutable.Map())

    allFilesToDelete.values.foreach(_.delete())

    if (allFilesIDsToProcess.nonEmpty) {
      import scala.collection.JavaConverters.asJavaCollectionConverter
      val filesToProcess: util.Collection[CommitLogFile] = allFilesIDsToProcess
        .map{case (id, _) => new CommitLogFile(allFiles(id).getFile.getPath)}
        .asJavaCollection

      val maxSize = scala.math.max(filesToProcess.size, queueSize)
      val commitLogQueue = new PriorityBlockingQueue[CommitLogStorage](maxSize)

      if (filesToProcess.isEmpty) commitLogQueue
      else if (commitLogQueue.addAll(filesToProcess)) commitLogQueue
      else throw new Exception("Something goes wrong here")
    } else {
      new PriorityBlockingQueue[CommitLogStorage](queueSize)
    }
  }
}