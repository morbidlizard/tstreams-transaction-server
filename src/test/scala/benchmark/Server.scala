package benchmark

import java.io.IOException
import java.util.concurrent.ArrayBlockingQueue

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.MessageToByteEncoder
import io.netty.handler.codec.bytes.ByteArrayDecoder
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import io.netty.util.ReferenceCountUtil
import org.apache.log4j.Logger
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TMemoryInputTransport
import test.Txn

object Server extends App {
  val logger = Logger.getLogger(getClass)
  val zkServers = "192.168.1.192:2181"
  val host = "localhost"
  val port = 8888
  val prefix = "/zk_test/global"

  try {
    val server = new TcpServer(zkServers, prefix, host, port)

    server.launch()
  } catch {
    case ex: IOException => logger.debug(s"Error: ${ex.getMessage}")
  }
}


class TcpServer(zkServers: String, prefix: String, host: String, port: Int) {
  private val logger = Logger.getLogger(getClass)
  private val retryPeriod = 5000
  private val masterNode = prefix + "/master"
  private val address = host + ":" + port
  private val out = new ArrayBlockingQueue[(ChannelHandlerContext, Array[Byte])](1000)

  def launch() = {
    val leader = new LeaderLatch(Set(zkServers), masterNode, address)
    leader.start()
    leader.takeLeadership(retryPeriod)
    logger.info(s"Launch a tcp server on: '$address'\n")
    val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)
    val workerGroup = new NioEventLoopGroup()
    try {
      val bootstrapServer = new ServerBootstrap()
      bootstrapServer.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new TcpServerChannelInitializer(out))


      new Thread(new Runnable {
        override def run() = {
          while (true) {
            val answer = out.take()
            val txn = decode(answer._2)
            answer._1.writeAndFlush(txn._1.isDefined && txn._2.isEmpty)
          }
        }

        def decode(bytes: Array[Byte]): Txn = {
          val protocolFactory = new TBinaryProtocol.Factory
          val buffer = new TMemoryInputTransport(bytes)
          val proto = protocolFactory.getProtocol(buffer)
          Txn.decode(proto)
        }
      }).start()

      val channel = bootstrapServer.bind(host, port).sync().channel()
      channel.closeFuture().sync()
    } finally {
      leader.close()
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}

class TcpServerChannelInitializer(out: ArrayBlockingQueue[(ChannelHandlerContext, Array[Byte])]) extends ChannelInitializer[SocketChannel] {

  def initChannel(channel: SocketChannel) = {
    channel.config().setTcpNoDelay(true)
    channel.config().setKeepAlive(true)
    channel.config().setTrafficClass(0x10)
    channel.config().setPerformancePreferences(0, 1, 0)

    val pipeline = channel.pipeline()

    pipeline.addLast("encoder", new BooleanEncoder())
    pipeline.addLast("decoder", new ByteArrayDecoder())
    pipeline.addLast("handler", new TransactionGenerator(out))
    pipeline.addLast()
  }
}

class BooleanEncoder extends MessageToByteEncoder[Boolean] {
  def encode(ctx: ChannelHandlerContext, msg: Boolean, out: ByteBuf): Unit = {
    out.writeBoolean(msg)
  }
}

@Sharable
class TransactionGenerator(out: ArrayBlockingQueue[(ChannelHandlerContext, Array[Byte])]) extends ChannelInboundHandlerAdapter {

  override def channelRead(ctx: ChannelHandlerContext, msg: Any) = {
    try {
      val serializedTxn = msg.asInstanceOf[Array[Byte]]
      out.offer((ctx, serializedTxn))
    } finally {
      ReferenceCountUtil.release(msg)
    }
  }

  /**
    * Exception handler that print stack trace and than close the connection when an exception is raised.
    *
    * @param ctx   Channel handler context
    * @param cause What has caused an exception
    */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause.printStackTrace()
    ctx.channel().close()
    ctx.channel().parent().close()
  }
}
