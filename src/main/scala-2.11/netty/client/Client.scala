package netty.client

import java.util.concurrent.{Callable, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger

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
  private val nextSeqId = new AtomicInteger(Int.MinValue)
  val workerGroup = new NioEventLoopGroup()
  val channel = {
    val b = new Bootstrap()
      .group(workerGroup)
      .channel(classOf[NioSocketChannel])
      .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .handler(new ClientInitializer)
    val f = b.connect(transactionServerListen, transactionServerPort).sync()
    f.channel()
  }

  val rand = scala.util.Random

  (0 to 10) foreach { _ =>
    val message = Descriptors.PutStream
      .encodeRequest(TransactionService.PutStream.Args(rand.nextInt(1000),"asd", rand.nextInt(1000), Some("asdas"),rand.nextInt(1000)))(nextSeqId.incrementAndGet())
    channel.writeAndFlush(message.toByteArray)
  }


//  class MyCallbale(var value: Any) extends Callable[Any] {
//    override def call(): Any = value
//  }
//
//  new ConcurrentHashMap[Int, MyCallbale]().put(123, new MyCallbale())


  channel.closeFuture().sync()
}
