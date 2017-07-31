package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.{RequestMessage, Protocol, ResponseMessage, SocketHostPortPair}
import com.bwsw.tstreamstransactionserver.netty.client.zk.ZKMasterInteractor
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.rpc.{TransactionService, TransportOptionsInfo}
import com.twitter.scrooge.ThriftStruct
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, EventLoopGroup}
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.ByteArrayEncoder
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContextExecutorService, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

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
                 requestIdToResponseMap: ConcurrentHashMap[Long, Promise[ByteBuf]],
                 context: ExecutionContextExecutorService) {

  private val logger =
    LoggerFactory.getLogger(this.getClass)

  private val isAuthenticating  =
    new java.util.concurrent.atomic.AtomicBoolean(false)

  @volatile private var currentToken: Int = -1
  @volatile private var messageSizeValidator: MessageSizeValidator =
    new MessageSizeValidator(Int.MaxValue, Int.MaxValue)



  private final def onRequestTimeoutDefaultBehaviour(): Unit = {
    TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
    onRequestTimeout
  }

  private val zkInteractor = new ZKMasterInteractor(
    zkConnection,
    zookeeperOptions.prefix,
    _ => {} /*reconnectToServer()*/,
    onZKConnectionStateChanged
  )

  private final def onServerConnectionLostDefaultBehaviour(): Unit = {
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

    if (logger.isWarnEnabled) {
      logger.warn(s"Server is unreachable. Retrying to reconnect server $connectionSocket.")
    }
  }

  private def handlers = {
    Seq(
      new ByteArrayEncoder(),
      new LengthFieldBasedFrameDecoder(
        Int.MaxValue,
        ResponseMessage.headerFieldSize,
        ResponseMessage.lengthFieldSize
      ),
      new ClientHandler(requestIdToResponseMap)
    )
  }

  private val nettyClient = new NettyConnection(
    workerGroup,
    handlers,
    clientOpts.connectionTimeoutMs,
    clientOpts.retryDelayMs,
    retrieveCurrentMaster(),
    {
      onServerConnectionLost
      Future {
        onServerConnectionLostDefaultBehaviour()
      }(context)
    }
  )

//  private def reconnectToServer() = {
//    if (nettyClient != null)
//      nettyClient.reconnect()
//  }

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
          else {
            val master =  masterOpt.get
            master
          }


      }
    }
    go(System.currentTimeMillis())
  }

  private def sendRequest[Req <: ThriftStruct, Rep <: ThriftStruct, A](message: RequestMessage,
                                                                       descriptor: Protocol.Descriptor[Req, Rep],
                                                                       f: Rep => A,
                                                                       previousException: Option[Throwable] = None,
                                                                       retryCount: Int = Int.MaxValue)
                                                                      (implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    val promise = Promise[ByteBuf]
    requestIdToResponseMap.put(message.id, promise)

    val channel = nettyClient.getChannel()
    channel.write(message.toByteArray)


    val responseFuture = TimeoutScheduler.withTimeout(
      promise.future.map { response =>
        requestIdToResponseMap.remove(message.id)
        f(descriptor.decodeResponse(response))
      }
    )(clientOpts.requestTimeoutMs.millis,
      message.id
    )(methodContext)

    channel.flush()

    responseFuture.recoverWith { case error =>
      requestIdToResponseMap.remove(message.id)
      val (currentException, counter) =
        checkError(error, previousException, retryCount)
      if (counter == 0) {
        Future.failed(currentException)
      }
      else {
        val messageId = requestIDGen.getAndIncrement()
        val newMessage = message.copy(
          id = messageId,
          token = getToken
        )
        sendRequest(newMessage, descriptor, f, Some(currentException), counter)
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

    if (getToken == -1)
      authenticate()

    messageSizeValidator.validateMessageSize(message)

    sendRequest(message, descriptor, f)
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

    sendRequest(message, descriptor, f)
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

  private final def checkError(currentException:  Throwable,
                               previousException: Option[Throwable],
                               retryCount: Int): (Throwable, Int) = {
    currentException match {
      case tokenException: TokenInvalidException =>
        authenticate()
        previousException match {
          case Some(_: TokenInvalidException) =>
            (tokenException, retryCount - 1)
          case _ =>
            (tokenException, clientOpts.requestTimeoutRetryCount)
        }

      case concreteThrowable: ServerUnreachableException =>
        scala.util.Try(onServerConnectionLostDefaultBehaviour())
        match {
          case scala.util.Failure(throwable) =>
            (throwable, 0)
          case scala.util.Success(_) =>
            (concreteThrowable, Int.MaxValue)
        }

      case concreteThrowable: RequestTimeoutException =>
        scala.util.Try(onRequestTimeoutDefaultBehaviour()) match {
          case scala.util.Failure(throwable) =>
            (throwable, 0)
          case scala.util.Success(_) =>
            previousException match {
              case Some(_: RequestTimeoutException) =>
                if (retryCount == Int.MaxValue) {
                  (concreteThrowable, clientOpts.requestTimeoutRetryCount)
                }
                else {
                  val updatedCounter = retryCount - 1
                  if (updatedCounter <= 0) {
                    nettyClient.reconnect()
                    (concreteThrowable, Int.MaxValue)
                  } else {
                    (concreteThrowable, updatedCounter)
                  }
                }
              case _ =>
                (concreteThrowable, Int.MaxValue)
            }
        }

      case otherThrowable =>
        (otherThrowable, 0)
    }
  }



  final def currentConnectionSocketAddress: Either[Throwable, Option[SocketHostPortPair]] =
    zkInteractor.getCurrentMaster


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

          val packageSizes = getMaxPackagesSizes()
          messageSizeValidator = new MessageSizeValidator(
            packageSizes.maxMetadataPackageSize,
            packageSizes.maxDataPackageSize
          )

          latch.countDown()
        }
      )(context)

      latch.await(
        clientOpts.requestTimeoutMs,
        TimeUnit.MILLISECONDS
      )

      isAuthenticating.set(false)
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

  final def getZKCheckpointGroupServerPrefix(): Option[String] = {
    if (logger.isInfoEnabled)
      logger.info("getMaxPackagesSizes method is invoked.")

    onShutdownThrowException()

    var prefix = Option.empty[String]

    val latch = new CountDownLatch(1)
    methodWithoutMessageSizeValidation[TransactionService.GetZKCheckpointGroupServerPrefix.Args, TransactionService.GetZKCheckpointGroupServerPrefix.Result, Unit](
      Protocol.GetZKCheckpointGroupServerPrefix,
      TransactionService.GetZKCheckpointGroupServerPrefix.Args(),
      x => {
        prefix = x.success
        latch.countDown()
      }
    )(context)

    latch.await(
      clientOpts.requestTimeoutMs,
      TimeUnit.MILLISECONDS
    )
    prefix
  }

  /** It Disconnects client from server slightly */
  def shutdown(): Unit = {
    if (nettyClient != null) nettyClient.stop()
    if (zkInteractor != null) zkInteractor.stop()
  }
}
