package com.bwsw.tstreamstransactionserver.netty.server

import java.util.concurrent.{ArrayBlockingQueue, Executors, TimeUnit}

import com.bwsw.commitlog.filesystem.CommitLogCatalogueByFolder
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable.InvalidSocketAddress
import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.CommitLogKey
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.sleepycat.je._
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.{ChannelOption, SimpleChannelInboundHandler}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContextExecutorService

class Server(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions,
             serverOpts: BootstrapOptions, serverReplicationOpts: ServerReplicationOptions,
             storageOpts: StorageOptions, berkeleyStorageOptions: BerkeleyStorageOptions, rocksStorageOpts: RocksStorageOptions, commitLogOptions: CommitLogOptions,
             packageTransmissionOpts: TransportOptions,
             serverHandler: (TransactionServer, ScheduledCommitLog, TransportOptions, ExecutionContextExecutorService, Logger) => SimpleChannelInboundHandler[Message] =
             (server, journaledCommitLogImpl, packageTransmissionOpts, context, logger) => new ServerHandler(server, journaledCommitLogImpl, packageTransmissionOpts, context, logger),
             timer: Time = new Time{}
            ) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val transactionServerSocketAddress = createTransactionServerAddress()
  if (!ZKLeaderClientToPutMaster.isValidSocketAddress(transactionServerSocketAddress._1, transactionServerSocketAddress._2))
    throw new InvalidSocketAddress(s"Invalid socket address ${transactionServerSocketAddress._1}:${transactionServerSocketAddress._2}")

  private def createTransactionServerAddress() = {
    (System.getenv("HOST"), System.getenv("PORT0")) match {
      case (host, port) if host != null && port != null && scala.util.Try(port.toInt).isSuccess => (host, port.toInt)
      case _ => (serverOpts.host, serverOpts.port)
    }
  }

  private val zk = scala.util.Try(
    new ZKLeaderClientToPutMaster(
      zookeeperOpts.endpoints,
      zookeeperOpts.sessionTimeoutMs,
      zookeeperOpts.connectionTimeoutMs,
      new RetryForever(zookeeperOpts.retryDelayMs),
      zookeeperOpts.prefix
    )) match {
    case scala.util.Success(client) => client
    case scala.util.Failure(throwable) =>
      shutdown()
      throw throwable
  }


  private val executionContext = new ServerExecutionContext(serverOpts.threadPool, berkeleyStorageOptions.berkeleyReadThreadPool,
    rocksStorageOpts.writeThreadPool, rocksStorageOpts.readThreadPool)
  private val transactionServer = new TransactionServer(executionContext, authOpts, storageOpts, rocksStorageOpts, timer)

  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean, func: => Unit): Long =
    transactionServer.notifyProducerTransactionCompleted(onNotificationCompleted, func)

  final def removeNotification(id: Long) = transactionServer.removeProducerTransactionNotification(id)

  final def notifyConsumerTransactionCompleted(onNotificationCompleted: ConsumerTransaction => Boolean, func: => Unit): Long =
    transactionServer.notifyConsumerTransactionCompleted(onNotificationCompleted, func)

  final def removeConsumerNotification(id: Long) = transactionServer.removeConsumerTransactionNotification(id)

  private val berkeleyWriterExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("BerkeleyWriter-%d").build())
  private val commitLogQueue = new CommitLogQueueBootstrap(1000, new CommitLogCatalogueByFolder(storageOpts.path + java.io.File.separatorChar + storageOpts.commitLogDirectory), transactionServer).fillQueue()


  /**
    * this variable is public for testing purposes only
    */
  val berkeleyWriter = new CommitLogToBerkeleyWriter(
    new RocksDbConnection(rocksStorageOpts, s"${storageOpts.path}${java.io.File.separatorChar}${storageOpts.commitLogRocksDirectory}"),
    commitLogQueue,
    transactionServer,
    commitLogOptions.incompleteCommitLogReadPolicy
  ) {
    override def getCurrentTime: Long = timer.getCurrentTime
  }

  private val fileIDGenerator = new zk.FileIDGenerator("/test_counter", 0L)
  private val scheduledCommitLogImpl = new ScheduledCommitLog(commitLogQueue, storageOpts, commitLogOptions, fileIDGenerator.increment) {
    override def getCurrentTime: Long = timer.getCurrentTime
  }

  private val commitLogTask = new Runnable {
    override def run(): Unit = {
      scheduledCommitLogImpl.run()
      berkeleyWriter.run()
    }
  }

  private val bossGroup = new EpollEventLoopGroup(1)
  private val workerGroup = new EpollEventLoopGroup()

  def start(): Unit = {
    try {
      berkeleyWriterExecutor.scheduleWithFixedDelay(commitLogTask, 0, commitLogOptions.commitLogCloseDelayMs, java.util.concurrent.TimeUnit.MILLISECONDS)

      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[EpollServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ServerInitializer(serverHandler(transactionServer, scheduledCommitLogImpl, packageTransmissionOpts, executionContext.context, logger)))
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)

      val f = b.bind(serverOpts.host, serverOpts.port).sync()

      zk.putSocketAddress(transactionServerSocketAddress._1, transactionServerSocketAddress._2)

      f.channel().closeFuture().sync()
    } finally {
      shutdown()
    }
  }

  def shutdown(): Unit = {
    if (bossGroup != null) {
      bossGroup.shutdownGracefully()
      bossGroup.terminationFuture()
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully()
      workerGroup.terminationFuture()
    }
    if (zk != null) zk.close()
    if (transactionServer != null) transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
    if (berkeleyWriterExecutor != null) {
      berkeleyWriterExecutor.shutdown()
      berkeleyWriterExecutor.awaitTermination(
        commitLogOptions.commitLogCloseDelayMs * 5,
        TimeUnit.MILLISECONDS
      )
      berkeleyWriter.closeRocksDB()
    }
    if (transactionServer != null) transactionServer.closeAllDatabases()
  }
}

class CommitLogQueueBootstrap(queueSize: Int, commitLogCatalogue: CommitLogCatalogueByFolder, transactionServer: TransactionServer) {
  def fillQueue(): ArrayBlockingQueue[String] = {
    val allFiles = commitLogCatalogue.listAllFilesAndTheirIDs().toMap

    import scala.collection.JavaConverters.asJavaCollectionConverter
    val allFilesIDsToProcess = allFiles.keys.toSeq diff getProcessedCommitLogFiles

    val filesToProcess = allFilesIDsToProcess.map(id => allFiles(id).getFile.getPath).sorted.asJavaCollection

    val maxSize = scala.math.max(filesToProcess.size, queueSize)
    val commitLogQueue = new ArrayBlockingQueue[String](maxSize)

    if (filesToProcess.isEmpty) commitLogQueue
    else if (commitLogQueue.addAll(filesToProcess)) commitLogQueue
    else throw new Exception("Something goes wrong here")
  }

  private def getProcessedCommitLogFiles: ArrayBuffer[Long] = {
    val keyFound = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val processedCommitLogFiles = scala.collection.mutable.ArrayBuffer[Long]()
    val cursor = transactionServer.commitLogDatabase.openCursor(new DiskOrderedCursorConfig().setKeysOnly(true))
    while (cursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      processedCommitLogFiles += CommitLogKey.keyToObject(keyFound).id
    }
    cursor.close()

    println(processedCommitLogFiles)
    processedCommitLogFiles
  }
}