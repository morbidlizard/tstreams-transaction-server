package com.bwsw.tstreamstransactionserver.netty.server

import java.util.concurrent.Executors

import com.bwsw.commitlog.CommitLog
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.JournaledCommitLogImpl
import com.bwsw.tstreamstransactionserver.options._
import com.bwsw.tstreamstransactionserver.zooKeeper.ZKLeaderClientToPutMaster
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelOption
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryNTimes
import org.slf4j.{Logger, LoggerFactory}

class Server(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, serverOpts: ServerOptions,
             storageOpts: StorageOptions, serverReplicationOpts: ServerReplicationOptions,
             rocksStorageOpts: RocksStorageOptions) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val transactionServerAddress = (System.getenv("HOST"), System.getenv("PORT0")) match {
    case (host, port) if host != null && port != null => s"$host:$port"
    case _ => s"${serverOpts.host}:${serverOpts.port}"
  }
  private val executionContext = new ServerExecutionContext(serverOpts.threadPool, storageOpts.berkeleyReadThreadPool,
    rocksStorageOpts.writeThreadPool, rocksStorageOpts.readThreadPoll)

  val zk = new ZKLeaderClientToPutMaster(zookeeperOpts.endpoints, zookeeperOpts.sessionTimeoutMs, zookeeperOpts.connectionTimeoutMs,
    new RetryNTimes(zookeeperOpts.retryCount, zookeeperOpts.retryDelayMs), zookeeperOpts.prefix)
  zk.putData(transactionServerAddress.getBytes())

  private val transactionServer = new TransactionServer(executionContext, authOpts, storageOpts, rocksStorageOpts)

  private val bossGroup = new EpollEventLoopGroup(1)
  private val workerGroup = new EpollEventLoopGroup()

  final private val scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("CommitLog-%d").build())
  private val journaledCommitLog= new JournaledCommitLogImpl(new CommitLog(Int.MaxValue, "/tmp"), transactionServer, scheduledExecutor)

  def start(): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[EpollServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ServerInitializer(transactionServer, journaledCommitLog, executionContext.context, logger))
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)


      val f = b.bind(serverOpts.host, serverOpts.port).sync()
      f.channel().closeFuture().sync()
    } finally {
      zk.close()
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
      transactionServer.shutdown()
      executionContext.shutdown()
    }
  }

  def shutdown() = {
    zk.close()
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
    transactionServer.shutdown()
    executionContext.shutdown()
  }
}
