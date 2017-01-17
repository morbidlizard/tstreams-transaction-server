package netty.server

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import netty.server.streamService.StreamServiceImpl
import netty.server.transactionMetaService.TransactionMetaServiceImpl
import org.apache.curator.retry.RetryNTimes
import zooKeeper.ZKLeaderServer

class Server extends TransactionServer{
  import configProperties.ServerConfig._
  val zk = new ZKLeaderServer(zkEndpoints,zkTimeoutSession,zkTimeoutConnection,
    new RetryNTimes(zkRetriesMax, zkTimeoutBetweenRetries),zkPrefix)
  zk.putData(transactionServerAddress.getBytes())

  private val bossGroup = new EpollEventLoopGroup(1)
  private val workerGroup = new EpollEventLoopGroup()

  def start(): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[EpollServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ServerInitializer)
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)


      val f = b.bind(transactionServerListen, transactionServerPort).sync()
      f.channel().closeFuture().sync()
    } finally {
      zk.close()
//      StreamServiceImpl.close()
//      TransactionMetaServiceImpl.close()
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
  def close() = {
    zk.close()
//    StreamServiceImpl.close()
//    TransactionMetaServiceImpl.close()
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
  }
}

object Server extends App{
  new Server().start()
}
