package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreamstransactionserver.configProperties.ClientExecutionContextGrid
import com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown
import com.bwsw.tstreamstransactionserver.netty.client.api.TTSClient
import com.bwsw.tstreamstransactionserver.netty.client.zk.ZKClient
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.rpc._
import com.twitter.scrooge.ThriftStruct
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import org.apache.commons.lang.SystemUtils
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.retry.RetryForever
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.Promise

class InetClientProxy(clientOpts: ConnectionOptions,
                      authOpts: AuthOptions,
                      zookeeperOptions: ZookeeperOptions,
                      curatorConnection: Option[CuratorFramework] = None)
  extends TTSClient
{
  private val logger =
    LoggerFactory.getLogger(this.getClass)

  private val isZKClientExternal =
    curatorConnection.isDefined

  private val isAuthenticated  =
    new java.util.concurrent.atomic.AtomicBoolean(false)

  @volatile private var isShutdown =
    false

  private val workerGroup: EventLoopGroup =
    if (SystemUtils.IS_OS_LINUX)
      new EpollEventLoopGroup()
    else
      new NioEventLoopGroup()


  private final val executionContext =
    new ClientExecutionContextGrid(clientOpts.threadPool)

  private final val context =
    executionContext.context

  private val zkConnection =
    curatorConnection.getOrElse {
      new ZKClient(
        zookeeperOptions.endpoints,
        zookeeperOptions.sessionTimeoutMs,
        zookeeperOptions.connectionTimeoutMs,
        new RetryForever(zookeeperOptions.retryDelayMs),
        zookeeperOptions.prefix
      ).client
    }


  private final val requestIdToResponseMap =
    new ConcurrentHashMap[Long, Promise[ThriftStruct]](
      20000,
      1.0f,
      clientOpts.threadPool
    )


  private def onShutdownThrowException(): Unit =
    if (isShutdown) throw ClientIllegalOperationAfterShutdown



  override def getCommitLogOffsets(): Future[CommitLogInfo] = ???

  override def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int): Future[Boolean] = ???

  override def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int): Future[Seq[Array[Byte]]] = ???

  override def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): Future[Int] = ???

  override def putStream(stream: StreamValue): Future[Int] = ???

  override def delStream(name: String): Future[Boolean] = ???

  override def getStream(name: String): Future[Option[rpc.Stream]] = ???

  override def checkStreamExists(name: String): Future[Boolean] = ???

  override def putProducerStateWithData(producerTransaction: ProducerTransaction, data: Seq[Array[Byte]], from: Int): Future[Boolean] = ???

  override def putSimpleTransactionAndData(streamID: Int, partition: Int, data: Seq[Array[Byte]]): Future[Long] = ???

  override def putSimpleTransactionAndDataWithoutResponse(streamID: Int, partition: Int, data: Seq[Array[Byte]]): Unit = ???

  override def getTransaction(): Future[Long] = ???

  override def getTransaction(timestamp: Long): Future[Long] = ???

  override def putTransactions(producerTransactions: Seq[ProducerTransaction], consumerTransactions: Seq[ConsumerTransaction]): Future[Boolean] = ???

  override def putProducerState(transaction: ProducerTransaction): Future[Boolean] = ???

  override def putTransaction(transaction: ConsumerTransaction): Future[Boolean] = ???

  override def openTransaction(streamID: Int, partitionID: Int, transactionTTLMs: Long): Future[Long] = ???

  override def getTransaction(streamID: Int, partition: Int, transaction: Long): Future[TransactionInfo] = ???

  override def getLastCheckpointedTransaction(streamID: Int, partition: Int): Future[Long] = ???

  override def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: Set[TransactionStates]): Future[ScanTransactionsInfo] = ???

  override def putConsumerCheckpoint(consumerTransaction: ConsumerTransaction): Future[Boolean] = ???

  override def getConsumerState(name: String, streamID: Int, partition: Int): Future[Long] = ???
}
