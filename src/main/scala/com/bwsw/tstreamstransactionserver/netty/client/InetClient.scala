package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.StampedLock

import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol, SocketHostPortPair}
import com.bwsw.tstreamstransactionserver.netty.client.zk.ZKMasterInteractor
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.rpc.{TransactionService, TransportOptionsInfo}
import com.twitter.scrooge.ThriftStruct
import io.netty.channel.EventLoopGroup
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContextExecutorService, Future, Promise}
import scala.concurrent.duration._

class InetClient(zookeeperOptions: ZookeeperOptions,
                 clientOpts: ConnectionOptions,
                 authOpts: AuthOptions,
                 onServerConnectionLost: => Unit,
                 onRequestTimeout: => Unit,
                 onZKConnectionStateChanged: ConnectionState => Unit,
                 workerGroup: EventLoopGroup,
                 isShutdown: => Boolean,
                 zkConnection: CuratorFramework,
                 requestIDGen: AtomicLong,
                 requestIdToResponseMap: ConcurrentHashMap[Long, Promise[ThriftStruct]],
                 context: ExecutionContextExecutorService) {

  private val logger =
    LoggerFactory.getLogger(this.getClass)

  private final def onServerConnectionLostDefaultBehaviour(connectionSocket: String): Unit = {
    if (logger.isWarnEnabled) {
      logger.warn(s"Server is unreachable. Retrying to reconnect server $connectionSocket.")
    }
    TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
    onServerConnectionLost
  }

  private final def onRequestTimeoutDefaultBehaviour(): Unit = {
    TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
    onRequestTimeout
  }

  private val zkInteractor = new ZKMasterInteractor(
    zkConnection,
    zookeeperOptions.prefix,
    _ => {},
    onZKConnectionStateChanged
  )

  private val nettyClient = new NettyConnectionHandler(
    workerGroup,
    new ClientInitializer(requestIdToResponseMap, context),
    clientOpts.connectionTimeoutMs,
    retrieveCurrentMaster(),
    {
      onServerConnectionLostDefaultBehaviour("")
      requestIdToResponseMap.forEach((t: Long, promise: Promise[ThriftStruct]) => {
        promise.tryFailure(new ServerUnreachableException(
          zkInteractor.getCurrentMaster
            .right.map(_.map(_.toString).getOrElse("NO_CONNECTION_SOCKET"))
            .right.getOrElse("NO_CONNECTION_SOCKET"))
        )
      })
    }
  )

  private def onShutdownThrowException(): Unit =
    if (isShutdown) throw ClientIllegalOperationAfterShutdown

  private final def retrieveCurrentMaster(): SocketHostPortPair = {
    @tailrec
    def go(timestamp: Long): SocketHostPortPair = {
      val master = zkInteractor.getCurrentMaster
      master match {
        case Left(throwable) =>
          if (logger.isWarnEnabled())
            logger.warn(throwable.getMessage)
          shutdown()
          throw throwable
        case Right(masterOpt) =>
          if (masterOpt.isEmpty) {
            val currentTime = System.currentTimeMillis()
            val timeDiff = scala.math.abs(currentTime - timestamp)
            if (logger.isInfoEnabled && timeDiff >= zookeeperOptions.retryDelayMs) {
              logger.info(s"Retrying to get master server from zookeeper servers: ${zookeeperOptions.endpoints}.")
              go(currentTime)
            }
            else
              go(timestamp)
          }
          else
            masterOpt.get
      }
    }
    go(System.currentTimeMillis())
  }

  private lazy val (maxMetadataPackageSize, maxDataPackageSize) = {
    val packageSizes = Await.result(
      getMaxPackagesSizes(),
      1000.milliseconds
    )
    (packageSizes.maxMetadataPackageSize, packageSizes.maxDataPackageSize)
  }
  private def validateMessageSize(message: Message): Unit = {
    message.method match {
      case Protocol.GetMaxPackagesSizes.methodID =>

      case Protocol.PutTransactionData.methodID if maxDataPackageSize != -1 =>
        if (message.length > maxDataPackageSize)
          throw new PackageTooBigException(s"Client shouldn't transmit amount of data which is greater " +
            s"than maxDataPackageSize ($maxDataPackageSize).")
      case _ if maxMetadataPackageSize != -1 =>
        if (message.length > maxMetadataPackageSize)  {
          throw new PackageTooBigException(s"Client shouldn't transmit amount of data which is greater " +
            s"than maxMetadataPackageSize ($maxMetadataPackageSize).")
        }
      case _ =>
    }
  }


  /** A general method for sending requests to a server and getting a response back.
    *
    * @param descriptor look at [[com.bwsw.tstreamstransactionserver.netty.Protocol]].
    * @param request    a request that client would like to send.
    * @return a response from server(however, it may return an exception from server).
    *
    */
  private final def method[Req <: ThriftStruct, Rep <: ThriftStruct, A](descriptor: Protocol.Descriptor[Req, Rep],
                                                                        request: Req,
                                                                        f: Rep => A
                                                                       )(implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    val messageId = requestIDGen.getAndIncrement()

    val message = descriptor.encodeRequestToMessage(request)(
      messageId,
      getToken,
      isFireAndForgetMethod = false
    )

    validateMessageSize(message)

    def go(message: Message,
           previousException: Option[Throwable] = None,
           retryCount: Int = Int.MaxValue): Future[A] = {
      val promise = Promise[ThriftStruct]
      requestIdToResponseMap.put(message.id, promise)

      val channel = nettyClient.getChannel()
      channel.write(message.toByteArray)

      val responseFuture = TimeoutScheduler.withTimeout(
        promise.future.map { response =>
          requestIdToResponseMap.remove(message.id)
          f(response.asInstanceOf[Rep])
        }
      )(methodContext, after = clientOpts.requestTimeoutMs.millis, message.id)

      channel.flush()

      responseFuture.recoverWith { case error =>
        requestIdToResponseMap.remove(message.id)
        val (currentException, _, counter) =
          checkError(error, previousException, retryCount)
        if (counter == 0) {
          Future.failed(error)
        }
        else {
          val messageId = requestIDGen.getAndIncrement()
          val newMessage = message.copy(
            id = messageId,
            token = getToken)
          go(newMessage, Some(currentException), counter)
        }
      }(methodContext)
    }

    go(message)
  }

  private final def checkError(currentException: Throwable,
                               previousException: Option[Throwable],
                               retryCount: Int): (Throwable, Throwable, Int) = {
    currentException match {
      case tokenException: TokenInvalidException =>
        authenticate()
        previousException match {
          case Some(exception: TokenInvalidException) =>
            (tokenException, tokenException, retryCount - 1)
          case _ =>
            (tokenException, tokenException, clientOpts.requestTimeoutRetryCount)
        }

      case concreteThrowable: ServerUnreachableException =>
        val addressOptOrZkException = currentConnectionSocketAddress
          .map(addressOpt => addressOpt.map(_.toString))

        scala.util.Try(onServerConnectionLostDefaultBehaviour(addressOptOrZkException
        match {
          case Right(connectionString) =>
            connectionString.getOrElse("")
          case Left(throwable) =>
            throw throwable
        }))
        match {
          case scala.util.Success(_) =>
            (concreteThrowable, concreteThrowable, Int.MaxValue)
          case scala.util.Failure(throwable) =>
            throw throwable
        }

      case concreteThrowable: RequestTimeoutException =>
        scala.util.Try(onRequestTimeoutDefaultBehaviour()) match {
          case scala.util.Success(_) =>
            previousException match {
              case Some(exception: RequestTimeoutException) =>
                if (retryCount == 0) {
                  nettyClient.reconnect()
                  (exception, concreteThrowable, Int.MaxValue)
                }
                else if (retryCount == Int.MaxValue)
                  (exception, concreteThrowable, clientOpts.requestTimeoutRetryCount)
                else {
                  (exception, concreteThrowable, retryCount - 1)
                }
              case _ =>
                (concreteThrowable, concreteThrowable, Int.MaxValue)
            }
          case scala.util.Failure(throwable) =>
            throw throwable
        }

      case otherThrowable =>
        throw otherThrowable
    }
  }



  final def currentConnectionSocketAddress: Either[Throwable, Option[SocketHostPortPair]] =
    zkInteractor.getCurrentMaster


  private val isAuthenticatedLock  =
    new StampedLock()

  @volatile private var currentToken: Int = -1

  @tailrec
  private final def getToken: Int = {
    val stamp = isAuthenticatedLock.tryOptimisticRead()
    val token = currentToken
    val isValid = isAuthenticatedLock.validate(stamp)
    isAuthenticatedLock.unlockRead(stamp)
    if (isValid)
      token
    else
      getToken
  }

  /** Retrieves a token for that allow a client send requests to server.
    *
    * @return Future of authenticate operation that can be completed or not. If it is completed it returns:
    *         1) token - for authorizing,
    *         maxDataPackageSize - max size of package into putTransactionData method with its arguments wrapped,
    *         maxMetadataPackageSize - max size of package into all kind of that operations with producer and consumer transactions methods with its arguments wrapped,
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         4) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  private def authenticate(): Unit = {
    if (logger.isInfoEnabled)
      logger.info("authenticate method is invoked.")

    onShutdownThrowException()

    if (!isAuthenticatedLock.isWriteLocked) {
      val stamp = isAuthenticatedLock.tryWriteLock()
      val authKey = authOpts.key
      val latch = new CountDownLatch(1)
      method[TransactionService.Authenticate.Args, TransactionService.Authenticate.Result, Unit](
        Protocol.Authenticate,
        TransactionService.Authenticate.Args(authKey),
        x => {
          val tokenFromServer = x.success.get
          currentToken = tokenFromServer
          isAuthenticatedLock.unlockWrite(stamp)
          latch.countDown()
        }
      )(context)
      latch.await(
        clientOpts.requestTimeoutMs,
        TimeUnit.MILLISECONDS
      )
    } else {
      while (isAuthenticatedLock.isWriteLocked){}
    }
  }

  private def getMaxPackagesSizes(): Future[TransportOptionsInfo] = {
    if (logger.isInfoEnabled)
      logger.info("getMaxPackagesSizes method is invoked.")

    onShutdownThrowException()

    method[TransactionService.GetMaxPackagesSizes.Args, TransactionService.GetMaxPackagesSizes.Result, TransportOptionsInfo](
      Protocol.GetMaxPackagesSizes,
      TransactionService.GetMaxPackagesSizes.Args(),
      x => x.success.get
    )(context)
  }

  /** It Disconnects client from server slightly */
  def shutdown(): Unit = {
    if (!isShutdown) {
      if (nettyClient != null)  nettyClient.stop()
      if (zkInteractor != null) zkInteractor.stop()
    }
  }
}
