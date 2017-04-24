package com.bwsw.tstreamstransactionserver.netty.server

import java.util
import java.util.concurrent.{Executors, PriorityBlockingQueue, TimeUnit}

import com.bwsw.commitlog.filesystem.{CommitLogCatalogue, CommitLogFile, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable.InvalidSocketAddress
import com.bwsw.tstreamstransactionserver.netty.{InetSocketAddressClass, Message}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService._
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.options.{CommonOptions, ServerOptions}
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.{ChannelOption, SimpleChannelInboundHandler}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever
import org.slf4j.{Logger, LoggerFactory}


class Server(authOpts: AuthOptions, zookeeperOpts: CommonOptions.ZookeeperOptions,
             serverOpts: BootstrapOptions, serverReplicationOpts: ServerReplicationOptions,
             storageOpts: StorageOptions, berkeleyStorageOptions: BerkeleyStorageOptions, rocksStorageOpts: RocksStorageOptions, commitLogOptions: CommitLogOptions,
             packageTransmissionOpts: TransportOptions, zookeeperSpecificOpts: ServerOptions.ZooKeeperOptions,
             serverHandler: (TransactionServer, ScheduledCommitLog, TransportOptions, Logger) => SimpleChannelInboundHandler[ByteBuf] =
             (server, journaledCommitLogImpl, packageTransmissionOpts, logger) => new ServerHandler(server, journaledCommitLogImpl, packageTransmissionOpts, logger),
             timer: Time = new Time{}
            ) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  @volatile private var isShutdown = false
  private val transactionServerSocketAddress = createTransactionServerAddress()
  private def createTransactionServerAddress() = {
    (System.getenv("HOST"), System.getenv("PORT0")) match {
      case (host, port) if host != null && port != null && scala.util.Try(port.toInt).isSuccess => (host, port.toInt)
      case _ => (serverOpts.host, serverOpts.port)
    }
  }
  if (!InetSocketAddressClass.isValidSocketAddress(serverOpts.host, serverOpts.port))
    throw new InvalidSocketAddress(s"Invalid socket address $serverOpts.localHost:$serverOpts.port")

  private val zk = scala.util.Try(
    new ZKClientServer(
      transactionServerSocketAddress._1,
      transactionServerSocketAddress._2,
      zookeeperOpts.endpoints,
      zookeeperOpts.sessionTimeoutMs,
      zookeeperOpts.connectionTimeoutMs,
      new RetryForever(zookeeperOpts.retryDelayMs)
    )) match {
    case scala.util.Success(client) => client
    case scala.util.Failure(throwable) =>
      shutdown()
      throw throwable
  }


  private val executionContext = new ServerExecutionContext(
    serverOpts.threadPool,
    berkeleyStorageOptions.berkeleyReadThreadPool,
    rocksStorageOpts.writeThreadPool,
    rocksStorageOpts.readThreadPool
  )
  private val transactionServer = new TransactionServer(executionContext, authOpts, storageOpts, rocksStorageOpts, timer)

  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean, func: => Unit): Long =
    transactionServer.notifyProducerTransactionCompleted(onNotificationCompleted, func)

  final def removeNotification(id: Long): Boolean = transactionServer.removeProducerTransactionNotification(id)

  final def notifyConsumerTransactionCompleted(onNotificationCompleted: ConsumerTransaction => Boolean, func: => Unit): Long =
    transactionServer.notifyConsumerTransactionCompleted(onNotificationCompleted, func)

  final def removeConsumerNotification(id: Long): Boolean = transactionServer.removeConsumerTransactionNotification(id)


  private val rocksDBCommitLog = new RocksDbConnection(rocksStorageOpts, s"${storageOpts.path}${java.io.File.separatorChar}${storageOpts.commitLogRocksDirectory}", commitLogOptions.commitLogFileTtlSec)
  private val (commitLogQueue, commitLogLastId) = {
    val queue = new CommitLogQueueBootstrap(30, new CommitLogCatalogue(storageOpts.path + java.io.File.separatorChar + storageOpts.commitLogDirectory), transactionServer)
    val lastFileIDBerkeley = transactionServer.getLastProcessedCommitLogFileID.getOrElse(-1L)

    val (priorityQueue, maxCommitLogFileID) = queue.fillQueue()

    val initGenFileID = scala.math.max(lastFileIDBerkeley, maxCommitLogFileID)

    val record = rocksDBCommitLog.getLastRecord
    record match {
      case Some((lastFileIDRocksBinary, _)) =>
        val lastFileIDRocks = FileKey.fromByteArray(lastFileIDRocksBinary).id
        (priorityQueue, scala.math.max(initGenFileID, lastFileIDRocks))

      case None =>
        (priorityQueue, initGenFileID)
    }
  }

  /**
    * this variable is public for testing purposes only
    */
  val berkeleyWriter = new CommitLogToBerkeleyWriter(
    rocksDBCommitLog,
    commitLogQueue,
    transactionServer,
    commitLogOptions.incompleteCommitLogReadPolicy
  ) {
    override def getCurrentTime: Long = timer.getCurrentTime
  }

  private val fileIDGenerator = new zk.FileIDGenerator(zookeeperSpecificOpts.counterPathFileIdGen, commitLogLastId)
  val scheduledCommitLogImpl = new ScheduledCommitLog(commitLogQueue, storageOpts, commitLogOptions, fileIDGenerator.increment) {
    override def getCurrentTime: Long = timer.getCurrentTime
  }


  private val berkeleyWriterExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("BerkeleyWriter-%d").build())
  private val commitLogCloseExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("CommitLogClose-%d").build())
  private val bossGroup = new EpollEventLoopGroup(1)
  private val workerGroup = new EpollEventLoopGroup()

  def start(): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[EpollServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ServerInitializer(serverHandler(transactionServer, scheduledCommitLogImpl, packageTransmissionOpts, logger), packageTransmissionOpts))
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)

      val f = b.bind(serverOpts.host, serverOpts.port).sync()
      berkeleyWriterExecutor.scheduleWithFixedDelay(scheduledCommitLogImpl, commitLogOptions.commitLogCloseDelayMs, commitLogOptions.commitLogCloseDelayMs, java.util.concurrent.TimeUnit.MILLISECONDS)
      commitLogCloseExecutor.scheduleWithFixedDelay(berkeleyWriter, 0, commitLogOptions.commitLogCloseDelayMs*11/10, java.util.concurrent.TimeUnit.MILLISECONDS)

      zk.putSocketAddress(zookeeperOpts.prefix)

      f.channel().closeFuture().sync()
    } finally {
      shutdown()
    }
  }

  def shutdown(): Unit = {
    if (!isShutdown) {
      isShutdown = true
      if (bossGroup != null) {
        bossGroup.shutdownGracefully()
        bossGroup.terminationFuture()
      }
      if (workerGroup != null) {
        workerGroup.shutdownGracefully()
        workerGroup.terminationFuture()
      }
      if (zk != null)
        zk.close()

      if (transactionServer != null)
        transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()

      if (berkeleyWriterExecutor != null) {
        berkeleyWriterExecutor.shutdown()
        berkeleyWriterExecutor.awaitTermination(
          commitLogOptions.commitLogCloseDelayMs * 5,
          TimeUnit.MILLISECONDS
        )
      }

      if (scheduledCommitLogImpl != null)
        scheduledCommitLogImpl.closeWithoutCreationNewFile()

      if (commitLogCloseExecutor != null) {
        commitLogCloseExecutor.shutdown()
        commitLogCloseExecutor.awaitTermination(
          commitLogOptions.commitLogCloseDelayMs * 5,
          TimeUnit.MILLISECONDS
        )
      }

      if (berkeleyWriter != null) {
        berkeleyWriter.run()
        berkeleyWriter.closeRocksDB()
        transactionServer.closeAllDatabases()
      }
    }
  }
}

class CommitLogQueueBootstrap(queueSize: Int, commitLogCatalogue: CommitLogCatalogue, transactionServer: TransactionServer) {
  def fillQueue(): (PriorityBlockingQueue[CommitLogStorage], Long) = {
    val allFiles = commitLogCatalogue.listAllFilesAndTheirIDs().toMap

    val berkeleyProcessedFileIDMax = transactionServer.getLastProcessedCommitLogFileID
    val (allFilesIDsToProcess, allFilesToDelete: Map[Long, CommitLogFile]) =
      if (berkeleyProcessedFileIDMax.isDefined)
        (allFiles.filterKeys(_ > berkeleyProcessedFileIDMax.get), allFiles.filterKeys(_ <= berkeleyProcessedFileIDMax.get))
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

      val maxCommitLogID = allFilesIDsToProcess.keys.max

      if (filesToProcess.isEmpty) (commitLogQueue, maxCommitLogID)
      else if (commitLogQueue.addAll(filesToProcess)) (commitLogQueue, maxCommitLogID)
      else throw new Exception("Something goes wrong here")
    } else {
      (new PriorityBlockingQueue[CommitLogStorage](queueSize), -1L)
    }
  }
}