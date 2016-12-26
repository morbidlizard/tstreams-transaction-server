package netty.server

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryNTimes
import transactionService.rpc.TransactionService
import zooKeeper.ZKLeaderServer

class Server extends TransactionServer{
  import configProperties.ServerConfig._
  val zk = new ZKLeaderServer(zkEndpoints,zkTimeoutSession,zkTimeoutConnection,
    new RetryNTimes(zkRetriesMax, zkTimeoutBetweenRetries),zkPrefix)
  zk.putData(transactionServerAddress.getBytes())

  def run(): Unit = {
    val bossGroup = new NioEventLoopGroup(1)
    val workerGroup = new NioEventLoopGroup()
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ServerInitializer)
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)

      val f = b.bind(transactionServerListen, transactionServerPort).sync()
      // Wait until the server socket is closed.
      // In this example, this does not happen, but you can do that to gracefully
      // shut down your server.
      f.channel().closeFuture().sync()
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}

object Server extends App{
  new Server().run()
}
