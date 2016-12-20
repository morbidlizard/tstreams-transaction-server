package benchmark

import java.io.IOException

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator}
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
  val zkServers = "176.120.25.19:2181"
  val host = "192.168.1.192"
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

  def launch() = {
    val leader = new LeaderLatch(Set(zkServers), masterNode, address)
    leader.start()
    leader.takeLeadership(retryPeriod)
    logger.info(s"Launch a tcp server on: '$address'\n")
    val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)
    val workerGroup = new NioEventLoopGroup()
    try {
      val bootstrapServer = new ServerBootstrap()
      bootstrapServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      bootstrapServer.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new TcpServerChannelInitializer())

      val channel = bootstrapServer.bind(host, port).sync().channel()
      channel.closeFuture().sync()
    } finally {
      leader.close()
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}

class TcpServerChannelInitializer() extends ChannelInitializer[SocketChannel] {

  def initChannel(channel: SocketChannel) = {
    channel.config().setTcpNoDelay(true)
    channel.config().setKeepAlive(true)
    channel.config().setTrafficClass(0x10)
    channel.config().setPerformancePreferences(0, 1, 0)

    val pipeline = channel.pipeline()

    pipeline.addLast("encoder", new BooleanEncoder())
    pipeline.addLast("decoder", new ByteArrayDecoder())
    pipeline.addLast("handler", new TransactionGenerator())
    pipeline.addLast()
  }
}

class BooleanEncoder extends MessageToByteEncoder[Boolean] {
  def encode(ctx: ChannelHandlerContext, msg: Boolean, out: ByteBuf): Unit = {
    out.writeBoolean(msg)
  }
}

@Sharable
class TransactionGenerator() extends ChannelInboundHandlerAdapter {

  override def channelRead(ctx: ChannelHandlerContext, msg: Any) = {
    try {
      val serializedTxn = msg.asInstanceOf[Array[Byte]]
      val txn = decode(serializedTxn)
      ctx.writeAndFlush(txn._1.isDefined && txn._2.isEmpty)
    } finally {
      ReferenceCountUtil.release(msg)
    }
  }

  def decode(bytes: Array[Byte]): Txn = {
    val protocolFactory = new TBinaryProtocol.Factory
    val buffer = new TMemoryInputTransport(bytes)
    val proto = protocolFactory.getProtocol(buffer)
    Txn.decode(proto)
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
