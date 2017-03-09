package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.zooKeeper.ZKLeaderClientToPutMaster
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelOption, SimpleChannelInboundHandler}
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutorService

class Server(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, serverOpts: BootstrapOptions,
             storageOpts: StorageOptions, serverReplicationOpts: ServerReplicationOptions,
             rocksStorageOpts: RocksStorageOptions, packageTransmissionOpts: PackageTransmissionOptions,
             serverHandler: (TransactionServer, PackageTransmissionOptions, ExecutionContextExecutorService, Logger) => SimpleChannelInboundHandler[Message] = (server, packageTransmissionOpts, context, logger) => new ServerHandler(server, packageTransmissionOpts, context, logger)) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val transactionServerSocketAddress = createTransactionServerAddress()

  val zk = new ZKLeaderClientToPutMaster(zookeeperOpts.endpoints, zookeeperOpts.sessionTimeoutMs, zookeeperOpts.connectionTimeoutMs,
    new RetryForever(zookeeperOpts.retryDelayMs), zookeeperOpts.prefix)
  zk.putSocketAddress(transactionServerSocketAddress._1, transactionServerSocketAddress._2)

  private val executionContext = new ServerExecutionContext(serverOpts.threadPool, storageOpts.berkeleyReadThreadPool,
    rocksStorageOpts.writeThreadPool, rocksStorageOpts.readThreadPool)
  private val transactionServer = new TransactionServer(executionContext, authOpts, storageOpts, rocksStorageOpts)

  private val bossGroup = new EpollEventLoopGroup(1)
  private val workerGroup = new EpollEventLoopGroup()

  private def createTransactionServerAddress() = {
    (System.getenv("HOST"), System.getenv("PORT0")) match {
      case (host, port) if host != null && port != null && scala.util.Try(port.toInt).isSuccess => (host, port.toInt)
      case _ => (serverOpts.host, serverOpts.port)
    }
  }

  def start(): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[EpollServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ServerInitializer(serverHandler(transactionServer, packageTransmissionOpts, executionContext.context, logger)))
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)


      val f = b.bind(serverOpts.host, serverOpts.port).sync()
      f.channel().closeFuture().sync()
    } finally {
      shutdown()
    }
  }

  def shutdown() = {
    zk.close()
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
    transactionServer.shutdown()
  }
}
