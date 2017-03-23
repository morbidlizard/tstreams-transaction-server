package com.bwsw.tstreamstransactionserver.netty.server

import java.util.concurrent.{ArrayBlockingQueue, Executors}

import com.bwsw.commitlog.filesystem.{CommitLogCatalogue, ICommitLogCatalogue}
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
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

class Server(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions,
             serverOpts: BootstrapOptions, serverReplicationOpts: ServerReplicationOptions,
             storageOpts: StorageOptions, berkeleyStorageOptions: BerkeleyStorageOptions, rocksStorageOpts: RocksStorageOptions, commitLogOptions: CommitLogOptions,
             packageTransmissionOpts: PackageTransmissionOptions,
             serverHandler: (TransactionServer, ScheduledCommitLog, PackageTransmissionOptions, ExecutionContextExecutorService, Logger) => SimpleChannelInboundHandler[Message] =
             (server, journaledCommitLogImpl, packageTransmissionOpts, context, logger) => new ServerHandler(server, journaledCommitLogImpl, packageTransmissionOpts, context, logger)) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val transactionServerSocketAddress = createTransactionServerAddress()

  private val zk = new ZKLeaderClientToPutMaster(zookeeperOpts.endpoints, zookeeperOpts.sessionTimeoutMs, zookeeperOpts.connectionTimeoutMs,
    new RetryForever(zookeeperOpts.retryDelayMs), zookeeperOpts.prefix)
  zk.putSocketAddress(transactionServerSocketAddress._1, transactionServerSocketAddress._2)

  private val executionContext = new ServerExecutionContext(serverOpts.threadPool, berkeleyStorageOptions.berkeleyReadThreadPool,
    rocksStorageOpts.writeThreadPool, rocksStorageOpts.readThreadPool)
  private val transactionServer = new TransactionServer(executionContext, authOpts, storageOpts, rocksStorageOpts)
  private val bossGroup = new EpollEventLoopGroup(1)
  private val workerGroup = new EpollEventLoopGroup()

  private val commitLogQueue = new CommitLogQueueBootstrap(1000, new CommitLogCatalogue(storageOpts.path), transactionServer).fillQueue()
  private val scheduledCommitLogImpl = new ScheduledCommitLog(commitLogQueue, storageOpts, commitLogOptions)
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
      berkeleyWriterExecutor.scheduleWithFixedDelay(berkeleyWriter, 0, commitLogOptions.commitLogToBerkeleyDBTaskDelayMs, java.util.concurrent.TimeUnit.MILLISECONDS)

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

class CommitLogQueueBootstrap(queueSize: Int, commitLogCatalogue: ICommitLogCatalogue, transactionServer: TransactionServer) {
  def fillQueue(): ArrayBlockingQueue[String] = {
    val allFilesOfCatalogues = commitLogCatalogue.catalogues.flatMap(_.listAllFiles().map(_.getFile().getPath))

    import scala.collection.JavaConverters.asJavaCollectionConverter
    val allProcessedFiles = (allFilesOfCatalogues diff getProcessedCommitLogFiles).sorted.asJavaCollection

    val maxSize = scala.math.max(allProcessedFiles.size(), queueSize)
    val commitLogQueue = new ArrayBlockingQueue[String](maxSize)

    if (allProcessedFiles.isEmpty) commitLogQueue
    else if (commitLogQueue.addAll(allProcessedFiles)) commitLogQueue
    else throw new Exception("Something goes wrong here")
  }

  private def getProcessedCommitLogFiles = {
    val commitLogDatabase = transactionServer.commitLogDatabase

    val keyFound = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val processedCommitLogFiles = scala.collection.mutable.ArrayBuffer[String]()
    val cursor = commitLogDatabase.openCursor(null, null)
    while (cursor.getNext(keyFound, dataFound, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
      processedCommitLogFiles += TimestampCommitLog.pathToObject(dataFound)
    }
    cursor.close()

    processedCommitLogFiles
  }
}