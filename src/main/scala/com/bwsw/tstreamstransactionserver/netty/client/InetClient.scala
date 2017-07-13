package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

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
import scala.concurrent.{ExecutionContextExecutorService, Future, Promise}
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

  @volatile private var currentToken: Int = -1


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

  private final def onServerConnectionLostDefaultBehaviour(): Unit = {
    val before = System.currentTimeMillis()

    val connectionSocket = zkInteractor.getCurrentMaster
      .right.map(_.map(_.toString).getOrElse("NO_CONNECTION_SOCKET"))
      .right.getOrElse("NO_CONNECTION_SOCKET")

    val requests = requestIdToResponseMap.elements()
    while (requests.hasMoreElements) {
      val request = requests.nextElement()
      request.tryFailure(new ServerUnreachableException(
        connectionSocket
      ))
    }

    onServerConnectionLost

    if (logger.isWarnEnabled) {
      logger.warn(s"Server is unreachable. Retrying to reconnect server $connectionSocket.")
    }

    val now = System.currentTimeMillis()
    val diff = now - before
    if (diff < clientOpts.connectionTimeoutMs)
      TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs - diff)
  }

  private val nettyClient = new NettyConnectionHandler(
    workerGroup,
    new ClientInitializer(requestIdToResponseMap, context),
    clientOpts.connectionTimeoutMs,
    retrieveCurrentMaster(), {
      Future {
        onServerConnectionLostDefaultBehaviour()
      }(context)
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

  private lazy val messageSizeValidator = {
    val packageSizes = getMaxPackagesSizes()
    new MessageSizeValidator(
      packageSizes.maxMetadataPackageSize,
      packageSizes.maxDataPackageSize
    )
  }

  private def sendRequest[Req <: ThriftStruct, Rep <: ThriftStruct, A](message: Message,
                                                                       f: Rep => A,
                                                                       previousException: Option[Throwable] = None,
                                                                       retryCount: Int = Int.MaxValue)
                                                                      (implicit methodContext: concurrent.ExecutionContext): Future[A] = {
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
        sendRequest(newMessage, f, Some(currentException), counter)
      }
    }(methodContext)
  }

  private def methodWithMessageSizeValidation[Req <: ThriftStruct, Rep <: ThriftStruct, A](descriptor: Protocol.Descriptor[Req, Rep],
                                                                                           request: Req,
                                                                                           f: Rep => A)
                                                                                          (implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    val messageId = requestIDGen.getAndIncrement()

    val message = descriptor.encodeRequestToMessage(request)(
      messageId,
      getToken,
      isFireAndForgetMethod = false
    )

    messageSizeValidator.validateMessageSize(message)

    sendRequest(message, f)
  }


  private def methodWithoutMessageSizeValidation[Req <: ThriftStruct, Rep <: ThriftStruct, A](descriptor: Protocol.Descriptor[Req, Rep],
                                                                                              request: Req,
                                                                                              f: Rep => A)
                                                                                             (implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    val messageId = requestIDGen.getAndIncrement()

    val message = descriptor.encodeRequestToMessage(request)(
      messageId,
      getToken,
      isFireAndForgetMethod = false
    )

    sendRequest(message, f)
  }


  /** A general method for sending requests to a server and getting a response back.
    *
    * @param descriptor look at [[com.bwsw.tstreamstransactionserver.netty.Protocol]].
    * @param request    a request that client would like to send.
    * @return a response from server(however, it may return an exception from server).
    *
    */
  final def method[Req <: ThriftStruct, Rep <: ThriftStruct, A](descriptor: Protocol.Descriptor[Req, Rep],
                                                                request: Req,
                                                                f: Rep => A
                                                               )(implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    methodWithMessageSizeValidation(
      descriptor,
      request,
      f
    )
  }

  @throws[TokenInvalidException]
  @throws[PackageTooBigException]
  final def methodFireAndForget[Req <: ThriftStruct](descriptor: Protocol.Descriptor[Req, _],
                                                     request: Req): Unit = {

    if (getToken == -1)
      authenticate()

    val messageId = requestIDGen.getAndIncrement()
    val message = descriptor.encodeRequestToMessage(request)(messageId, getToken, isFireAndForgetMethod = true)

    if (logger.isDebugEnabled) logger.debug(Protocol.methodWithArgsToString(messageId, request))
    messageSizeValidator.validateMessageSize(message)

    val channel = nettyClient.getChannel()
    channel.writeAndFlush(message.toByteArray, channel.voidPromise())
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
        scala.util.Try(onServerConnectionLostDefaultBehaviour()) match {
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


  private val isAuthenticating  =
    new java.util.concurrent.atomic.AtomicBoolean(false)


  private final def getToken: Int = {
    currentToken
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

    val needToAuthenticate =
      isAuthenticating.compareAndSet(false, true)

    if (needToAuthenticate) {
      val authKey = authOpts.key
      val latch = new CountDownLatch(1)
      methodWithoutMessageSizeValidation[TransactionService.Authenticate.Args, TransactionService.Authenticate.Result, Unit](
        Protocol.Authenticate,
        TransactionService.Authenticate.Args(authKey),
        x => {
          val tokenFromServer = x.success.get
          currentToken = tokenFromServer
          isAuthenticating.set(false)
          latch.countDown()
        }
      )(context)
      latch.await(
        clientOpts.requestTimeoutMs,
        TimeUnit.MILLISECONDS
      )
    } else {
      while (isAuthenticating.get()) {}
    }
  }

  private def getMaxPackagesSizes(): TransportOptionsInfo = {
    if (logger.isInfoEnabled)
      logger.info("getMaxPackagesSizes method is invoked.")

    onShutdownThrowException()

    val latch = new CountDownLatch(1)
    var transportOptionsInfo: TransportOptionsInfo = null
    methodWithoutMessageSizeValidation[TransactionService.GetMaxPackagesSizes.Args, TransactionService.GetMaxPackagesSizes.Result, Unit](
      Protocol.GetMaxPackagesSizes,
      TransactionService.GetMaxPackagesSizes.Args(),
      x => {
        transportOptionsInfo = x.success.get
        latch.countDown()
      }
    )(context)

    latch.await(
      clientOpts.requestTimeoutMs,
      TimeUnit.MILLISECONDS
    )
    transportOptionsInfo
  }

  /** It Disconnects client from server slightly */
  def shutdown(): Unit = {
    if (!isShutdown) {
      if (nettyClient != null)  nettyClient.stop()
      if (zkInteractor != null) zkInteractor.stop()
    }
  }
}
