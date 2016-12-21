package benchmark

import java.io.{File, IOException}
import java.nio.ByteBuffer

import com.sleepycat.je._
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
import test.{States, Txn}

object Server extends App {
  val logger = Logger.getLogger(getClass)
  val zkServers = "176.120.25.19:2181"
  val host = "192.168.1.192"
  val port = 8888
  val prefix = "/zk_test/global/tmp"

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
    val envConf = new EnvironmentConfig()
    envConf.setAllowCreate(true)
    envConf.setDurability(Durability.COMMIT_NO_SYNC)
    envConf.setTransactional(true)
    envConf.setConfigParam("je.log.fileMax", "100000000")

    val env: Environment = new Environment(new File("/tmp/test/"), envConf)

    val dbConf = new DatabaseConfig()
    dbConf.setAllowCreate(true)
    dbConf.setTransactional(true)

    val myDb: Database = env.openDatabase(null, "db", dbConf)
    val myDbOpen = env.openDatabase(null, "db_open", dbConf)

    val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)
    val workerGroup = new NioEventLoopGroup()
    try {
      val bootstrapServer = new ServerBootstrap()
      bootstrapServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      bootstrapServer.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new TcpServerChannelInitializer(env, myDb))

      val channel = bootstrapServer.bind(host, port).sync().channel()
      channel.closeFuture().sync()
    } finally {

      myDb.close()
      myDbOpen.close()
      env.close()
      leader.close()
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}

class TcpServerChannelInitializer(env: Environment, myDb: Database) extends ChannelInitializer[SocketChannel] {

  def initChannel(channel: SocketChannel) = {
    channel.config().setTcpNoDelay(true)
    channel.config().setKeepAlive(true)
    channel.config().setTrafficClass(0x10)
    channel.config().setPerformancePreferences(0, 1, 0)

    val pipeline = channel.pipeline()

    pipeline.addLast("encoder", new BooleanEncoder())
    pipeline.addLast("decoder", new ByteArrayDecoder())
    pipeline.addLast("handler", new TransactionGenerator(env, myDb))
    pipeline.addLast()
  }
}

class BooleanEncoder extends MessageToByteEncoder[Boolean] {
  def encode(ctx: ChannelHandlerContext, msg: Boolean, out: ByteBuf): Unit = {
    out.writeBoolean(msg)
  }
}

@Sharable
class TransactionGenerator(env: Environment, myDb: Database) extends ChannelInboundHandlerAdapter {

  override def channelRead(ctx: ChannelHandlerContext, msg: Any) = {
    try {
      val serializedTxn = msg.asInstanceOf[Array[Byte]]
      val txn = decode(serializedTxn)._1.get

      if (txn._4 != States.Invalid) {
        val k = new DatabaseEntry(ByteBuffer.allocate(32).put(txn._1.getBytes).putInt(txn._2).putLong(txn._3).array())
        val v = new DatabaseEntry(ByteBuffer.allocate(16).putInt(txn._4.getValue()).putInt(txn._5).putLong(txn._6).array())
        val transaction = env.beginTransaction(null, null)
        val status = myDb.put(transaction, k, v)
        transaction.commit()
        ctx.writeAndFlush(status == OperationStatus.SUCCESS)
      } else ctx.writeAndFlush(true)
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
