package netty.client

import configProperties.ServerConfig.{transactionServerListen, transactionServerPort}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{ChannelInitializer, ChannelOption}
import io.netty.handler.codec.bytes.ByteArrayEncoder
import netty.{Descriptors, Message}
import transactionService.rpc.TransactionService

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

  val rand = scala.util.Random

  (0 to 10) foreach { _ =>
    val bytes = Descriptors.PutStream
      .encodeRequest(TransactionService.PutStream.Args(rand.nextInt(1000),"asd", rand.nextInt(1000), Some("asdas"),rand.nextInt(1000)))
    val size = bytes.length
    val message = Message(size, bytes).toByteArray
    channel.writeAndFlush(message)
  }

  channel.closeFuture().sync()
}
