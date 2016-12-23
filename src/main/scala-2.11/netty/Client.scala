package netty

import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import configProperties.ServerConfig.{transactionServerListen, transactionServerPort}
import io.netty.handler.codec.bytes.ByteArrayEncoder

object Client extends App {
  val workerGroup = new NioEventLoopGroup()
  val channel = {
    val b = new Bootstrap()
      .group(workerGroup)
      .channel(classOf[NioSocketChannel])
      .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .handler(new ChannelInitializer[SocketChannel](){
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline()
            .addLast(new ByteArrayEncoder())
           // .addLast(new ClientHandler())
        }
      })
    val f = b.connect(transactionServerListen, transactionServerPort).sync()
    f.channel()
  }

  (1019 to 4096 by 1019) foreach { size =>
    val message = Message(size, Descriptor.PutStream, Array.fill(size)(Byte.MaxValue)).toByteArray
    channel.writeAndFlush(message)
  }

  channel.closeFuture().sync()
}
