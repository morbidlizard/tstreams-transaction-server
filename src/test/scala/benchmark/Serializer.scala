package benchmark

import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.Channel
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder
import io.netty.util.ReferenceCountUtil
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.leader
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TMemoryBuffer
import org.apache.zookeeper.KeeperException
import test.{ProducerTxn, States, Txn}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object MultipleClients {
  private val clients = 150
  private val clientThreads = ArrayBuffer[Thread]()

  def main(args: Array[String]): Unit = {
    (1 to clients).foreach(x => {
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          new TcpClientExample().main(Array())
        }
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}

class TcpClientExample {
  val zkServers = Array("176.120.25.19:2181")
  val prefix = "/zk_test/global"

  val options = new TcpClientOptions()
    .setZkServers(zkServers)
    .setPrefix(prefix)

  def main(args: Array[String]): Unit = {
    val client = new TcpClient(options)
    //val consoleReader = new BufferedReader(new InputStreamReader(System.in))
    var i = 0
    val t0 = System.currentTimeMillis()
    while (i <= 1000000) {
      //while (consoleReader.readLine() != null) {
      //logger.debug("send request")

      val producerTxn = ProducerTxn("stream", 1, System.nanoTime(), States.Opened, 1, 1000L)
      val txn = Txn(Some(producerTxn), None)
      client.get(txn)
      //      if (i % 10000 == 0) println(client.get(txn))
      //      else client.get(txn)
      i += 1
    }
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    println("OPS: " + 1000000 / ((t1 - t0) / 1000))
    client.close()
  }
}

import java.util.concurrent.ArrayBlockingQueue

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel


class TcpClient(options: TcpClientOptions) {
  private val out = new ArrayBlockingQueue[Boolean](1)
  private var channel: Channel = null
  private val workerGroup = new NioEventLoopGroup()
  private val bootstrap = new Bootstrap().option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
  private val (host, port) = getMasterAddress()
  private val protocolFactory = new TBinaryProtocol.Factory

  createChannel()

  private def getMasterAddress() = {
    val leader = new LeaderLatch(Set(options.zkServers), options.prefix + "/master")
    val leaderInfo = leader.getLeaderInfo()
    val address = leaderInfo.split(":")
    leader.close()

    (address(0), address(1).toInt)
  }

  private def createChannel() = {
    bootstrap.group(workerGroup)
      .channel(classOf[NioSocketChannel])
      .handler(new TcpClientChannelInitializer(out))

    channel = bootstrap.connect(host, port).sync().channel()
  }

  def get(txn: Txn) = {
    channel.writeAndFlush(encode(txn))
    val answer = out.take()

    answer
  }

  private def encode(txn: Txn): Array[Byte] = {
    val buffer = new TMemoryBuffer(1024)
    val proto = protocolFactory.getProtocol(buffer)
    Txn.encode(txn, proto)

    buffer.getArray
  }

  def close() = {
    workerGroup.shutdownGracefully()
  }
}

class TcpClientOptions() {
  var zkServers = "127.0.0.1:2181"
  var prefix = ""
  var retryPeriod = 5000
  var retryCount = 5

  def setZkServers(hosts: Array[String]): TcpClientOptions = {
    zkServers = hosts.mkString(",")
    this
  }

  def setPrefix(pref: String): TcpClientOptions = {
    prefix = pref
    this
  }

  def setRetryPeriod(period: Int): TcpClientOptions = {
    retryPeriod = period
    this
  }

  def setRetryCount(count: Int): TcpClientOptions = {
    retryCount = count
    this
  }
}

class LeaderLatch(zkServers: Set[String], masterNode: String, id: String = "") {
  private val servers = zkServers.mkString(",")
  private val curatorClient = createCuratorClient()
  createMasterNode()
  private val leaderLatch = new leader.LeaderLatch(curatorClient, masterNode, id)
  private var isStarted = false

  private def createCuratorClient() = {
    val curatorClient = CuratorFrameworkFactory.newClient(servers, 5000, 5000, new ExponentialBackoffRetry(1000, 3))
    curatorClient.start()
    curatorClient.getZookeeperClient.blockUntilConnectedOrTimedOut()
    curatorClient
  }

  private def createMasterNode() = {
    val doesPathExist = Option(curatorClient.checkExists().forPath(masterNode))
    if (doesPathExist.isEmpty) curatorClient.create.creatingParentsIfNeeded().forPath(masterNode)
  }

  def start() = {
    leaderLatch.start()
    isStarted = true
  }

  def takeLeadership(delay: Long) = {
    while (!hasLeadership) {
      Thread.sleep(delay)
    }
  }

  def getLeaderInfo() = {
    var leaderInfo = getLeaderId()
    while (leaderInfo == "") {
      leaderInfo = getLeaderId()
      Thread.sleep(50)
    }

    leaderInfo
  }

  @tailrec
  private def getLeaderId(): String = {
    try {
      leaderLatch.getLeader.getId
    } catch {
      case e: KeeperException =>
        Thread.sleep(50)
        getLeaderId()
    }
  }

  def hasLeadership() = {
    leaderLatch.hasLeadership
  }

  def close() = {
    if (isStarted) leaderLatch.close()
    curatorClient.close()
  }
}

class TcpClientChannelInitializer(out: ArrayBlockingQueue[Boolean]) extends ChannelInitializer[SocketChannel] {

  def initChannel(channel: SocketChannel) = {
    channel.config().setTcpNoDelay(true)
    channel.config().setKeepAlive(true)
    channel.config().setTrafficClass(0x10)
    channel.config().setPerformancePreferences(0, 1, 0)

    val pipeline = channel.pipeline()


    pipeline.addLast("encoder", new ByteArrayEncoder())
    pipeline.addLast("handler", new TcpClientChannel(out))
  }
}


@Sharable
class TcpClientChannel(out: ArrayBlockingQueue[Boolean]) extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: Any) = {
    try {
      val serializedId = msg.asInstanceOf[ByteBuf]
      out.offer(serializedId.readBoolean())
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
    ctx.close()
  }
}

