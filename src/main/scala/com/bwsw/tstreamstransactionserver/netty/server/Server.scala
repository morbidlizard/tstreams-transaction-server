package com.bwsw.tstreamstransactionserver.netty.server

import java.io.File
import java.util.concurrent.{ArrayBlockingQueue, Executors}

import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLogImpl}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TimestampCommitLog
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.zooKeeper.ZKLeaderClientToPutMaster
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.sleepycat.je._
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.{ChannelOption, SimpleChannelInboundHandler}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutorService

class Server(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, serverOpts: BootstrapOptions,
             storageOpts: StorageOptions, serverReplicationOpts: ServerReplicationOptions,
             rocksStorageOpts: RocksStorageOptions, commitLogOptions: CommitLogOptions,
             packageTransmissionOpts: PackageTransmissionOptions,
             serverHandler: (TransactionServer, ScheduledCommitLogImpl, PackageTransmissionOptions, ExecutionContextExecutorService, Logger) => SimpleChannelInboundHandler[Message] =
             (server, journaledCommitLogImpl, packageTransmissionOpts, context, logger) => new ServerHandler(server, journaledCommitLogImpl, packageTransmissionOpts, context, logger)) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val transactionServerSocketAddress = createTransactionServerAddress()

  private val zk = new ZKLeaderClientToPutMaster(zookeeperOpts.endpoints, zookeeperOpts.sessionTimeoutMs, zookeeperOpts.connectionTimeoutMs,
    new RetryForever(zookeeperOpts.retryDelayMs), zookeeperOpts.prefix)
  zk.putSocketAddress(transactionServerSocketAddress._1, transactionServerSocketAddress._2)

  private val executionContext = new ServerExecutionContext(serverOpts.threadPool, storageOpts.berkeleyReadThreadPool,
    rocksStorageOpts.writeThreadPool, rocksStorageOpts.readThreadPool)
  private val transactionServer = new TransactionServer(executionContext, authOpts, storageOpts, rocksStorageOpts)

  private val bossGroup = new EpollEventLoopGroup(1)
  private val workerGroup = new EpollEventLoopGroup()
  private val commitLogQueue = new CommitLogQueueBootstrap(1000).fillQueue()
  private val scheduledCommitLogImpl = new ScheduledCommitLogImpl(commitLogQueue, commitLogOptions)
  private val berkeleyWriter = new CommitLogToBerkeleyWriter(commitLogQueue, transactionServer, commitLogOptions.incompleteCommitLogReadPolicy)
  private val berkeleyWriterExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("BerkeleyWriter-%d").build())

  private def createTransactionServerAddress() = {
    (System.getenv("HOST"), System.getenv("PORT0")) match {
      case (host, port) if host != null && port != null && scala.util.Try(port.toInt).isSuccess => (host, port.toInt)
      case _ => (serverOpts.host, serverOpts.port)
    }
  }

  def start(): Unit = {
    try {
      berkeleyWriterExecutor.scheduleWithFixedDelay(berkeleyWriter, 0, 2, java.util.concurrent.TimeUnit.SECONDS)

      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[EpollServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ServerInitializer(serverHandler(transactionServer, scheduledCommitLogImpl, packageTransmissionOpts, executionContext.context, logger)))
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)


      val f = b.bind(serverOpts.host, serverOpts.port).sync()
      f.channel().closeFuture().sync()
    } finally {
      shutdown()
    }
  }

  def shutdown() = {
    berkeleyWriterExecutor.shutdown()
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
    zk.close()
    transactionServer.shutdown()
  }
}

class CommitLogQueueBootstrap(queueSize: Int) {
  private val commitLogQueue = new ArrayBlockingQueue[String](queueSize)

  def fillQueue(): ArrayBlockingQueue[String] = commitLogQueue ///todo implementation. blocked by #60

  ////code from ScheduledCommitLogImpl
  def getProcessedCommitLogFiles = {
    val directory: File = null //FileUtils.createDirectory(transactionServer.storageOpts.metadataDirectory, transactionServer.storageOpts.path)
    val defaultDurability = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE)
    val transactionMetaEnvironment = {
      val environmentConfig = new EnvironmentConfig()
        .setAllowCreate(true)
        .setTransactional(true)
        .setSharedCache(true)
      //    config.berkeleyDBJEproperties foreach {
      //      case (name, value) => environmentConfig.setConfigParam(name,value)
      //    } //todo it will be deprecated soon

      environmentConfig.setDurabilityVoid(defaultDurability)
      new Environment(directory, environmentConfig)
    }

    val commitLogDatabase = {
      val dbConfig = new DatabaseConfig()
        .setAllowCreate(true)
        .setTransactional(true)
      val storeName = "CommitLogStore"
      transactionMetaEnvironment.openDatabase(null, storeName, dbConfig)
    }

    val keyFound = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val processedCommitLogFiles = scala.collection.mutable.ArrayBuffer[String]()

    val cursor = commitLogDatabase.openCursor(null, null)

    while (cursor.getNext(keyFound, dataFound, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
      processedCommitLogFiles += TimestampCommitLog.pathToObject(dataFound)
    }
    cursor.close()

    commitLogDatabase.close()
    transactionMetaEnvironment.close()

    processedCommitLogFiles
  }

  private val catalogue = new CommitLogCatalogue("/tmp", new java.util.Date(System.currentTimeMillis()))
  //TODO if there in no directory exist before method is called exception will be thrown
  //  catalogue.listAllFiles() foreach (x => println(x.getFile().getAbsolutePath))
  //  println(getProcessedCommitLogFiles)
}