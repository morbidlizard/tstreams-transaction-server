package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits._
import com.bwsw.tstreamstransactionserver.configProperties.ClientExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable
import com.bwsw.tstreamstransactionserver.exception.Throwable.{RequestTimeoutException, _}
import com.bwsw.tstreamstransactionserver.netty._
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.rpc.{TransactionService, _}
import com.twitter.scrooge.ThriftStruct
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.retry.RetryForever
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise}
import scala.concurrent.duration._


/** A client who connects to a server.
  *
  *
  */
class Client(clientOpts: ConnectionOptions, authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions) {
  @volatile private[client] var isShutdown = false
  private def onShutdownThrowException(): Unit =
    if (isShutdown) throw ClientIllegalOperationAfterShutdown

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** A special context for making requests asynchronously, although they are processed sequentially;
    * If's for: putTransaction, putTransactions, setConsumerState.
    */
  private[client] final val processTransactionsPutOperationPool = ExecutionContext(1, "ClientTransactionPool-%d")

  private final val executionContext = new ClientExecutionContext(clientOpts.threadPool)

  private final val context = executionContext.context
  private final val contextForProducerTransactions = processTransactionsPutOperationPool.getContext

  private final val zkListener = new ConnectionStateListener {
    override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
      onZKConnectionStateChangedDefaultBehaviour(newState)
    }
  }

  private final val zKLeaderClient = new ZKLeaderClientToGetMaster(zookeeperOpts.endpoints,
    zookeeperOpts.sessionTimeoutMs, zookeeperOpts.connectionTimeoutMs,
    new RetryForever(zookeeperOpts.retryDelayMs), zookeeperOpts.prefix, zkListener)
  zKLeaderClient.start()

  private final def onZKConnectionStateChangedDefaultBehaviour(newState: ConnectionState): Unit = {
    newState match {
      case ConnectionState.LOST => zKLeaderClient.master = None
      case _ => ()
    }
    onZKConnectionStateChanged(newState)
  }

  private final val reqIdToRep= new ConcurrentHashMap[Long, ScalaPromise[ThriftStruct]](15000, 1.0f, clientOpts.threadPool)

  private val workerGroup: EventLoopGroup = new EpollEventLoopGroup()
  private val bootstrap = new Bootstrap()
    .group(workerGroup)
    .channel(classOf[EpollSocketChannel])
    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)
    .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, int2Integer(clientOpts.connectionTimeoutMs))
    .handler(new ClientInitializer(reqIdToRep, this, context))

  @volatile private var channel: Channel = connect()

  @tailrec
  final private def connect(): Channel = {
    val (listen, port) = getInetAddressFromZookeeper(10)
    val newConnection = bootstrap.connect(listen, port)
    scala.util.Try(newConnection.sync().channel()) match {
      case scala.util.Success(channelToUse) =>
        channelToUse
      case scala.util.Failure(throwable) =>
        if (throwable.isInstanceOf[java.util.concurrent.RejectedExecutionException])
          throw ClientIllegalOperationAfterShutdown
        else {
          onServerConnectionLostDefaultBehaviour("")
          connect()
        }
    }
  }


  final def currentConnectionSocketAddress: Option[InetSocketAddressClass] = zKLeaderClient.master


  private[client] def reconnect(): Unit = {
    channel = connect()
  }

  /** Retries to get ipAddres:port from zooKeeper server.
    *
    * @param times how many times try to get ipAddres:port from zooKeeper server.
    */
  @tailrec
  @throws[ZkGetMasterException]
  private def getInetAddressFromZookeeper(times: Int): (String, Int) = {
    if (times > 0 && zKLeaderClient.master.isEmpty) {
      TimeUnit.MILLISECONDS.sleep(zookeeperOpts.retryDelayMs)
      if (logger.isInfoEnabled) logger.info(s"Retrying to get master server from zookeeper servers: ${zookeeperOpts.endpoints}.")
      getInetAddressFromZookeeper(times - 1)
    } else {
      zKLeaderClient.master match {
        case Some(master) => (master.address, master.port)
        case None =>
          val throwable = new ZkGetMasterException(zookeeperOpts.endpoints)
          if (logger.isWarnEnabled()) logger.warn(throwable.getMessage)
          shutdown()
          throw throwable
      }
    }
  }

  private final val nextSeqId = new AtomicLong(1L)

  /** A general method for sending requests to a server and getting a response back.
    *
    * @param descriptor look at [[com.bwsw.tstreamstransactionserver.netty.Descriptors]].
    * @param request    a request that client would like to send.
    * @return a response from server(however, it may return an exception from server).
    *
    */
  @throws[ServerUnreachableException]
  private final def method[Req <: ThriftStruct, Rep <: ThriftStruct](descriptor: Descriptors.Descriptor[Req, Rep], request: Req)
                                                              (implicit methodContext: concurrent.ExecutionContext): ScalaFuture[Rep] = {
    if (channel != null && channel.isActive) {
      val messageId = nextSeqId.getAndIncrement()
      val promise = ScalaPromise[ThriftStruct]
      val message = descriptor.encodeRequest(request)(messageId, token)
      validateMessageSize(message)

      if (logger.isDebugEnabled) logger.debug(Descriptors.methodWithArgsToString(messageId, request))

      reqIdToRep.put(messageId, promise)
      channel.write(message.toByteArray)

      val responseFuture = TimeoutScheduler.withTimeout(promise.future.map { response =>
        ScalaFuture(reqIdToRep.remove(messageId))(context)
        response.asInstanceOf[Rep]
      })(methodContext, after = clientOpts.requestTimeoutMs.millis, messageId).recoverWith { case error =>
        ScalaFuture(reqIdToRep.remove(messageId))(context)
        ScalaFuture.failed(error)
      }(methodContext)

      channel.flush()
      responseFuture
    } else ScalaFuture.failed(new ServerUnreachableException(currentConnectionSocketAddress.toString))
  }

  private def validateMessageSize(message: Message): Unit = {
    if (maxMetadataPackageSize != -1 && maxDataPackageSize != -1) {
      if (message.length > maxMetadataPackageSize || message.length > maxDataPackageSize) {
        throw new PackageTooBigException(s"Client shouldn't transmit amount of data which is greater " +
          s"than maxMetadataPackageSize ($maxMetadataPackageSize) or maxDataPackageSize ($maxDataPackageSize).")
      }
    }
  }

  private final def retry[Req, Rep](f: => ScalaFuture[Rep])(previousException: Throwable, retryCount: Int, retryContext: concurrent.ExecutionContext): ScalaFuture[Rep] = {
    def helper(throwable: Throwable, retryCount: Int): ScalaFuture[Rep] = {
      if (retryCount > 0) {
        if (throwable.getClass equals previousException.getClass)
          retry(f)(throwable, retryCount, retryContext)
        else
          tryCompleteRequest(f)(retryContext)
      } else {
        if (throwable.getClass equals classOf[RequestTimeoutException]) {
          channel.close().sync()
          reconnect()
          tryCompleteRequest(f)(retryContext)
        } else ScalaFuture.failed(throwable)
      }
    }

    f.recoverWith {
      case tokenInvalidThrowable: TokenInvalidException =>
        if (logger.isWarnEnabled)
          logger.warn(s"Token $token isn't valid. Retrying to get one from $currentConnectionSocketAddress.")

        authenticate().flatMap{_ =>
          TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
          helper(tokenInvalidThrowable, retryCount - 1)
        }(retryContext)
      case serverUnreachableThrowable: ServerUnreachableException =>
        scala.util.Try(onServerConnectionLostDefaultBehaviour(currentConnectionSocketAddress.toString)) match {
          case scala.util.Success(_) => helper(serverUnreachableThrowable, retryCount)
          case scala.util.Failure(throwable) => ScalaFuture.failed(throwable)
        }
      case requestTimeout: RequestTimeoutException =>
        scala.util.Try(onRequestTimeoutDefaultBehaviour()) match {
          case scala.util.Success(_) => helper(requestTimeout, retryCount - 1)
          case scala.util.Failure(throwable) => ScalaFuture.failed(throwable)
        }
      case otherThrowable =>
        ScalaFuture.failed(otherThrowable)
    }(retryContext)
  }

  private final def tryCompleteRequest[Req, Rep](f: => ScalaFuture[Rep])(implicit retryContext: concurrent.ExecutionContext) = {
    f.recoverWith {
      case concreteThrowable: TokenInvalidException =>
        logger.warn(s"Token $token isn't valid. Retrying to get one from $currentConnectionSocketAddress.")

        authenticate().flatMap{ _ =>
          TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
          retry(f)(concreteThrowable, clientOpts.requestTimeoutRetryCount, retryContext)
        }(retryContext)

      case concreteThrowable: ServerUnreachableException =>
        scala.util.Try(onServerConnectionLostDefaultBehaviour(currentConnectionSocketAddress.toString)) match {
          case scala.util.Success(_) => retry(f)(concreteThrowable, Int.MaxValue, retryContext)
          case scala.util.Failure(throwable) => ScalaFuture.failed(throwable)
        }
      case concreteThrowable: RequestTimeoutException =>
        scala.util.Try(onRequestTimeoutDefaultBehaviour()) match {
          case scala.util.Success(_) => retry(f)(concreteThrowable, clientOpts.requestTimeoutRetryCount, retryContext)
          case scala.util.Failure(throwable) => ScalaFuture.failed(throwable)
        }
      case otherThrowable =>
        ScalaFuture.failed(otherThrowable)
    }(retryContext)
  }

  private final def onServerConnectionLostDefaultBehaviour(connectionSocket: String): Unit = {
    if (logger.isWarnEnabled) {
      logger.warn(s"${Throwable.serverUnreachableExceptionMessage}. Retrying to reconnect server $connectionSocket.")
    }
    TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
    onServerConnectionLost()
  }

  private final def onRequestTimeoutDefaultBehaviour(): Unit = {
    TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
    onRequestTimeout()
  }

  protected def onZKConnectionStateChanged(newState: ConnectionState): Unit = {}

  protected def onServerConnectionLost(): Unit = {}

  protected def onRequestTimeout(): Unit = {}


  @volatile private var token: Int = -1
  @volatile private var maxMetadataPackageSize: Int = -1
  @volatile private var maxDataPackageSize: Int = -1
  authenticate()


  /** Retrieving an offset between last processed commit log file and current commit log file where a server writes data.
    *
    * @return Future of getCommitLogOffsets operation that can be completed or not. If it is completed it returns:
    *         1)Thrift Struct [[com.bwsw.tstreamstransactionserver.rpc.CommitLogInfo]] which contains:
    *         currentProcessedCommitLog   - the one which is currently relayed with background worker
    *         currentConstructedCommitLog - the one which is currently under write routine
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server.
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path.
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getCommitLogOffsets(): ScalaFuture[com.bwsw.tstreamstransactionserver.rpc.CommitLogInfo] = {
    if (logger.isDebugEnabled()) logger.debug(s"Calling method 'getCommitLogOffsets' to get offsets.")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.GetCommitLogOffsets,
        TransactionService.GetCommitLogOffsets.Args()
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }


  /** Putting a stream on a server by primitive type parameters.
    *
    * @param stream      a name of stream.
    * @param partitions  a number of stream partitions.
    * @param description a description of stream.
    * @return Future of putStream operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if stream is persisted by a server or FALSE if there is a stream with such name on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path.
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         5) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): ScalaFuture[Boolean] = {
    if (logger.isDebugEnabled()) logger.debug(s"Putting stream $stream with $partitions partitions, ttl $ttl and description.")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.PutStream,
        TransactionService.PutStream.Args(stream, partitions, description, ttl)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }

  /** Putting a stream on a server by Thrift Stream structure.
    *
    * @param stream an object of Thrift Stream [[com.bwsw.tstreamstransactionserver.rpc.Stream]] structure.
    * @return Future of putStream operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if stream is persisted by a server or FALSE if there is a stream with such name on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putStream(stream: com.bwsw.tstreamstransactionserver.rpc.Stream): ScalaFuture[Boolean] = {
    if (logger.isDebugEnabled()) logger.debug(s"Putting stream ${stream.name} with ${stream.partitions} partitions, ttl ${stream.ttl} and description.")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.PutStream,
        TransactionService.PutStream.Args(stream.name, stream.partitions, stream.description, stream.ttl)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }

  /** Deleting a stream by name on a server.
    *
    * @param stream a name of stream.
    * @return Future of putStream operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if stream is removed by a server or FALSE if a stream is already deleted or there in no such stream on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream name has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def delStream(stream: String): ScalaFuture[Boolean] = {
    if (logger.isDebugEnabled) logger.debug(s"Deleting stream $stream.")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.DelStream,
        TransactionService.DelStream.Args(stream)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }

  /** Deleting a stream by Thrift Stream [[com.bwsw.tstreamstransactionserver.rpc.Stream]] structure on a server.
    *
    * @param stream a name of stream.
    * @return Future of delStream operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if stream is removed by a server or FALSE if a stream is already deleted or there in no such stream on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         4) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def delStream(stream: com.bwsw.tstreamstransactionserver.rpc.Stream): ScalaFuture[Boolean] = {
    if (logger.isDebugEnabled()) logger.debug(s"Deleting stream ${stream.name}.")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.DelStream,
        TransactionService.DelStream.Args(stream.name)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }

  /** Retrieving a stream from a server by it's name.
    *
    * @param stream a name of stream.
    * @return Future of getStream operation that can be completed or not. If it is completed it returns:
    *         1) Thrift Stream [[com.bwsw.tstreamstransactionserver.rpc.Stream]] if stream  is retrieved from a server or throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]];
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getStream(stream: String): ScalaFuture[com.bwsw.tstreamstransactionserver.rpc.Stream] = {
    if (logger.isDebugEnabled()) logger.debug(s"Retrieving stream $stream.")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.GetStream,
        TransactionService.GetStream.Args(stream)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }


  /** Checks by a stream's name that stream saved in database on server.
    *
    * @param stream a name of stream.
    * @return Future of checkStreamExists operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if stream is exists in a server database or FALSE if a it's not;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream name has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def checkStreamExists(stream: String): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info(s"Checking stream $stream on existence...")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.CheckStreamExists,
        TransactionService.CheckStreamExists.Args(stream)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }


  /** Puts producer and consumer transactions on a server; it's implied there were persisted streams on a server transactions belong to, otherwise
    * the exception would be thrown.
    *
    * @param producerTransactions some collections of producer transactions [[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]].
    * @param consumerTransactions some collections of consumer transactions [[com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction]].
    * @return Future of putTransactions operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put these transaction to commit log for next processing, otherwise FALSE;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package with collection of producer and consumer transactions has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def putTransactions(producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction],
                      consumerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction]): ScalaFuture[Boolean] = ScalaFuture{
    val transactions =
      (producerTransactions map (txn => Transaction(Some(txn), None))) ++
        (consumerTransactions map (txn => Transaction(None, Some(txn))))

    if (logger.isDebugEnabled)
      logger.debug(s"putTransactions method is invoked: $transactions.")

    onShutdownThrowException()

    tryCompleteRequest(
      method(
        Descriptors.PutTransactions,
        TransactionService.PutTransactions.Args(transactions)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }(contextForProducerTransactions).flatten

  /** Puts producer transaction on a server; it's implied there was persisted stream on a server transaction belong to, otherwise
    * the exception would be thrown.
    *
    * @param transaction a producer transaction [[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]].
    * @return Future of putProducerState operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put producer transaction to commit log for next processing, otherwise FALSE;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putProducerState(transaction: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction): ScalaFuture[Boolean] = ScalaFuture{
    if (logger.isDebugEnabled) logger.debug(s"Putting producer transaction ${transaction.transactionID} with state ${transaction.state} to stream ${transaction.stream}, partition ${transaction.partition}")
    val producerTransactionToTransaction = Transaction(Some(transaction), None)
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.PutTransaction,
        TransactionService.PutTransaction.Args(producerTransactionToTransaction)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }(contextForProducerTransactions).flatten

  /** Puts consumer transaction on a server; it's implied there was persisted stream on a server transaction belong to, otherwise
    * the exception would be thrown.
    *
    * @param transaction a consumer transactions.
    * @return Future of putTransaction operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put consumer transaction to commit log for next processing;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putTransaction(transaction: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction): ScalaFuture[Boolean] = ScalaFuture{
    if (logger.isDebugEnabled()) logger.debug(s"Putting consumer transaction ${transaction.transactionID} with name ${transaction.name} to stream ${transaction.stream}, partition ${transaction.partition}")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.PutTransaction,
        TransactionService.PutTransaction.Args(Transaction(None, Some(transaction)))
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }(contextForProducerTransactions).flatten

  /** Puts 'simplified' producer transaction that already has Chekpointed state with it's data
    *
    * @param stream      a name of stream.
    * @param partition   a partition of stream.
    * @param transaction a transaction id.
    * @param data a producer transaction data.
    * @param from a left bound to put producer transaction data to start with.
    * @return Future of putSimpleTransactionAndData operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if transaction is persisted in commit log file for next processing and it's data is persisted successfully too, otherwise FALSE.
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putSimpleTransactionAndData(stream: String, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = ScalaFuture{
    if (logger.isDebugEnabled) logger.debug(s"Putting 'lightweight' producer transaction $transaction to stream $stream, partition $partition with data: $data")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.PutSimpleTransactionAndData,
        TransactionService.PutSimpleTransactionAndData.Args(stream, partition, transaction, data, from)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }(contextForProducerTransactions).flatten

  /** Retrieves a producer transaction by id
    *
    * @param stream      a name of stream.
    * @param partition   a partition of stream.
    * @param transaction a transaction id.
    * @return Future of getTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Thrift TransactionInfo [[com.bwsw.tstreamstransactionserver.rpc.TransactionInfo]] which contains about whether transaction exists and producer transaction itself on case of existence;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getTransaction(stream: String, partition: Int, transaction: Long): ScalaFuture[TransactionInfo] = {
    if (logger.isDebugEnabled()) logger.debug(s"Retrieving a producer transaction on partition '$partition' of stream '$stream' by id '$transaction'")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.GetTransaction,
        TransactionService.GetTransaction.Args(stream, partition, transaction)
      )(context).flatMap(x =>
        if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message))
        else ScalaFuture.successful(x.success.get)
      )(context)
    )(context)
  }


  /** Retrieves last checkpointed transaction in a specific stream on certain partition; If the result is -1 it will mean there is no checkpointed transaction at all.
    *
    * @param stream    a name of stream.
    * @param partition a partition of stream.
    * @return Future of getLastCheckpointedTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Last producer transaction ID which state is "Checkpointed" on certain stream on certain partition.
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getLastCheckpointedTransaction(stream: String, partition: Int): ScalaFuture[Long] = {
    if (logger.isDebugEnabled()) logger.debug(s"Retrieving a last checkpointed transaction on partition '$partition' of stream '$stream")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.GetLastCheckpointedTransaction,
        TransactionService.GetLastCheckpointedTransaction.Args(stream, partition)
      )(context).flatMap(x =>
        if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message))
        else ScalaFuture.successful(x.success.getOrElse(-1L))
      )(context)
    )(context)
  }


  /** Retrieves all producer transactions in a specific range [from, to]; it's assumed that "from" and "to" are both positive.
    *
    * @param stream    a name of stream.
    * @param partition a partition of stream.
    * @param from      an inclusive bound to start with.
    * @param to        an inclusive bound to end with.
    * @return Future of scanTransactions operation that can be completed or not. If it is completed it returns:
    *         1) Thrift struct [[com.bwsw.tstreamstransactionserver.rpc.ScanTransactionsInfo]]
    *         that contains information about last opened(LT) transaction on certain stream on certain partition and
    *         producer transactions in range:
    *         If LT < from: it result is (LT, empty_collection_of_producer_transactions);
    *         If from <= LT < to: result is (LT, collection of producer transactions in range [from, LT] until producer transaction with state "Opened" met);
    *         If LT >= to: result is (LT, collection of producer transactions in range[from, to] until producer transaction with state "Opened" met).
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  @throws[Exception]
  def scanTransactions(stream: String, partition: Int, from: Long, to: Long, lambda: ProducerTransaction => Boolean = txn => true): ScalaFuture[ScanTransactionsInfo] = {
    require(from >= 0 && to >= 0, s"Calling method scanTransactions requires that bounds: 'from' and 'to' are both positive(actually from and to are: [$from, $to])")
    if (to < from) {
      onShutdownThrowException()
      val lastOpenedTransactionID = -1L
      ScalaFuture.successful(ScanTransactionsInfo(lastOpenedTransactionID, collection.immutable.Seq()))
    }
    else {
      if (logger.isDebugEnabled()) logger.debug(s"Retrieving producer transactions on stream $stream in range [$from, $to]")
      onShutdownThrowException()
      tryCompleteRequest(
        method(
          Descriptors.ScanTransactions,
          TransactionService.ScanTransactions.Args(stream, partition, from, to, ObjectSerializer.serialize(lambda))
        )(context).flatMap(x =>
          if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message))
          else ScalaFuture.successful(x.success.get)
        )(context)
      )(context)
    }
  }


  /** Putting any binary data on server to a specific stream, partition, transaction id of producer transaction.
    *
    * @param stream      a stream.
    * @param partition   a partition of stream.
    * @param transaction a transaction ID.
    * @param data        a data to persist.
    * @param from        an inclusive bound to strat with.
    * @return Future of putTransactionData operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if producer transaction data persisted on server successfully, otherwise FALSE.
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putTransactionData(stream: String, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    if (logger.isDebugEnabled) logger.debug(s"Putting transaction data to stream $stream, partition $partition, transaction $transaction.")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.PutTransactionData,
        TransactionService.PutTransactionData.Args(stream, partition, transaction, data, from)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }

  /** Putting any binary data and setting transaction state on server.
    *
    * @param producerTransaction a producer transaction contains all necessary information for persisting data.
    * @param data                a data to persist.
    * @return Future of putProducerStateWithData operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put producer transaction to commit log for next processing and producer transaction data persisted on server successfully,
    *         otherwise FALSE(operation isn't atomic, it's splitted to 2 operations: first putting producer state, then its data);
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putProducerStateWithData(producerTransaction: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = ScalaFuture{
    putTransactionData(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, data, from)
      .flatMap(_ => putProducerState(producerTransaction))(context)
  }(contextForProducerTransactions).flatten


  /** Retrieves all producer transactions binary data in a specific range [from, to]; it's assumed that "from" and "to" are both positive.
    *
    * @param stream      a stream
    * @param partition   a partition of stream.
    * @param transaction a transaction id.
    * @param from        an inclusive bound to strat with.
    * @param to          an inclusive bound to end with.
    * @return Future of getTransactionData operation that can be completed or not. If it is completed it returns:
    *         1) Collection of binary data in range [from, to] on certain stream on certain partition if data presents.
    *         otherwise FALSE(operation isn't atomic, it's splitted to 2 operations: first putting producer state, then its data);
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getTransactionData(stream: String, partition: Int, transaction: Long, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0, s"Calling method getTransactionData requires that bounds: 'from' and 'to' are both positive(actually from and to are: [$from, $to])")
    if (to < from) {
      onShutdownThrowException()
      ScalaFuture.successful(Seq[Array[Byte]]())
    }
    else {
      if (logger.isDebugEnabled) logger.debug(s"Retrieving producer transaction data from stream $stream, partition $partition, transaction $transaction in range [$from, $to].")
      onShutdownThrowException()
      tryCompleteRequest(
        method(
          Descriptors.GetTransactionData,
          TransactionService.GetTransactionData.Args(stream, partition, transaction, from, to)
        )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(byteBuffersToSeqArrayByte(x.success.get)))(context)
      )(context)
    }
  }


  /** Puts/Updates a consumer state on a specific stream, partition, transaction id on a server.
    *
    * @param consumerTransaction a consumer transaction contains all necessary information for putting/updating it's state.
    * @return Future of putConsumerCheckpoint operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put consumer transaction to commit log for next processing, otherwise FALSE;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putConsumerCheckpoint(consumerTransaction: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction): ScalaFuture[Boolean] = ScalaFuture{
    onShutdownThrowException()
    if (logger.isDebugEnabled())
      logger.debug(s"Setting consumer state ${consumerTransaction.name} on stream ${consumerTransaction.stream}, partition ${consumerTransaction.partition}, transaction ${consumerTransaction.transactionID}.")
    tryCompleteRequest(
      method(
        Descriptors.PutConsumerCheckpoint,
        TransactionService.PutConsumerCheckpoint.Args(consumerTransaction.name, consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID)
      )(contextForProducerTransactions).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(processTransactionsPutOperationPool.getContext)
    )(contextForProducerTransactions)
  }(contextForProducerTransactions).flatten

  /** Retrieves a consumer state on a specific consumer transaction name, stream, partition from a server; If the result is -1 it will mean there is no checkpoint at all.
    *
    * @param name      a consumer transaction name.
    * @param stream    a stream.
    * @param partition a partition of the stream.
    * @return Future of getConsumerState operation that can be completed or not. If it is completed it returns:
    *         1) transaction ID on certain stream on certatin partition if it exists, otherwise -1L
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getConsumerState(name: String, stream: String, partition: Int): ScalaFuture[Long] = {
    if (logger.isDebugEnabled) logger.debug(s"Retrieving a transaction by consumer $name on stream $stream, partition $partition.")
    onShutdownThrowException()
    tryCompleteRequest(
      method(
        Descriptors.GetConsumerState,
        TransactionService.GetConsumerState.Args(name, stream, partition)
      )(context).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(context)
    )(context)
  }


  /** Retrieves a token for that allow a client send requests to server.
    *
    * @return Future of authenticate operation that can be completed or not. If it is completed it returns:
    *         1) token - for authorizing,
    *            maxDataPackageSize - max size of package into putTransactionData method with its arguments wrapped,
    *            maxMetadataPackageSize - max size of package into all kind of that operations with producer and consumer transactions methods with its arguments wrapped,
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         4) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  private def authenticate(): ScalaFuture[Unit] = {
    if (logger.isInfoEnabled) logger.info("authenticate method is invoked.")
    onShutdownThrowException()
    val authKey = authOpts.key
    method(Descriptors.Authenticate, TransactionService.Authenticate.Args(authKey))(context)
      .map(x => {
        val authInfo = x.success.get
        token = authInfo.token
        maxDataPackageSize = authInfo.maxDataPackageSize
        maxMetadataPackageSize = authInfo.maxMetadataPackageSize
      })(context)
  }

  /** It Disconnects client from server slightly */
  def shutdown(): Unit = {
    if (!isShutdown) {
      isShutdown = true
      if (workerGroup != null) {
        workerGroup.shutdownGracefully()
        workerGroup.terminationFuture()
      }
      if (channel != null) channel.closeFuture()
      zKLeaderClient.close()
      processTransactionsPutOperationPool.stopAccessNewTasks()
      processTransactionsPutOperationPool.awaitAllCurrentTasksAreCompleted()
      executionContext.stopAccessNewTasksAndAwaitCurrentTasksToBeCompleted()
    }
  }
}