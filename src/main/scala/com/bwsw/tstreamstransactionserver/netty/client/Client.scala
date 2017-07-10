/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid
import com.bwsw.tstreamstransactionserver.`implicit`.Implicits._
import com.bwsw.tstreamstransactionserver.configProperties.ClientExecutionContextGrid
import com.bwsw.tstreamstransactionserver.exception.Throwable
import com.bwsw.tstreamstransactionserver.exception.Throwable.{RequestTimeoutException, _}
import com.bwsw.tstreamstransactionserver.netty._
import com.bwsw.tstreamstransactionserver.netty.client.api.TTSClient
import com.bwsw.tstreamstransactionserver.netty.client.zk.{ZKClient, ZKMiddleware}
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.rpc.{TransactionService, _}
import com.twitter.scrooge.ThriftStruct
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Await, Future => ScalaFuture, Promise => ScalaPromise}


/** A client who connects to a server.
  *
  *
  */
class Client(clientOpts: ConnectionOptions,
             authOpts: AuthOptions,
             zookeeperOptions: ZookeeperOptions,
             curatorConnection: Option[CuratorFramework] = None)
  extends TTSClient
{
  @volatile private[client] var isShutdown = false

  private def onShutdownThrowException(): Unit =
    if (isShutdown) throw ClientIllegalOperationAfterShutdown


  private val logger = LoggerFactory.getLogger(this.getClass)

  private val isAuthenticated  = new java.util.concurrent.atomic.AtomicBoolean(false)
  private val isReconnected    = new java.util.concurrent.atomic.AtomicBoolean(false)

  /** A special context for making requests asynchronously, although they are processed sequentially;
    * If's for: putTransaction, putTransactions, setConsumerState.
    */
  private[client] final val processTransactionsPutOperationPool = ExecutionContextGrid("ClientTransactionPool-%d")

  private final val executionContext = new ClientExecutionContextGrid(clientOpts.threadPool)

  private final val context = executionContext.context
  private final val contextForProducerTransactions = processTransactionsPutOperationPool.getContext


  private val zkInteractor = new ZKMiddleware(
    curatorConnection,
    zookeeperOptions,
    _ => {},
    onZKConnectionStateChanged
  )

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


  final def currentConnectionSocketAddress: Either[Throwable, Option[SocketHostPortPair]] =
    zkInteractor.getCurrentMaster

  private final val reqIdToRep = new ConcurrentHashMap[Long, ScalaPromise[ThriftStruct]](
    20000,
    1.0f,
    clientOpts.threadPool
  )

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
    val master = retrieveCurrentMaster()
    val (listen, port) = (master.address, master.port)
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


  private[client] def reconnect(): Unit = {
    val isConnected = isReconnected.getAndSet(true)
    if (!isConnected) {
      channel.closeFuture().sync()
      channel = connect()
      isReconnected.set(false)
    } else {
      TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
    }
  }

  private final val nextSeqId = new AtomicLong(1L)

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
                                                                       )(implicit methodContext: concurrent.ExecutionContext): ScalaFuture[A] = {

    val messageId = nextSeqId.getAndIncrement()
    val message = descriptor.encodeRequestToMessage(request)(messageId, token, isFireAndForgetMethod = false)
    validateMessageSize(message)

    def go(message: Message,
           previousException: Option[Throwable] = None, retryCount: Int = Int.MaxValue): ScalaFuture[A] = {
      if (!channel.isActive) {
        val error = new ServerUnreachableException(currentConnectionSocketAddress.toString)
        val (currentException, _, counter) = checkError(error, previousException, retryCount)
        go(message, Some(currentException), counter)
      }
      else {
        val promise = ScalaPromise[ThriftStruct]
        reqIdToRep.put(message.id, promise)
        channel.write(message.toByteArray)

        val responseFuture = TimeoutScheduler.withTimeout(promise.future.map { response =>
          reqIdToRep.remove(message.id)
          f(response.asInstanceOf[Rep])
        })(methodContext, after = clientOpts.requestTimeoutMs.millis, message.id)
          .recoverWith { case error =>
            reqIdToRep.remove(message.id)
            val (currentException, _, counter) = checkError(error, previousException, retryCount)
            if (counter == 0) {
              ScalaFuture.failed(error)
            }
            else {
              val messageId = nextSeqId.getAndIncrement()
              val newMessage = message.copy(
                id = messageId,
                token = token)
              go(newMessage, Some(currentException), counter)
            }
          }(methodContext)

        channel.flush()
        responseFuture
      }
    }

    go(message)
  }


  @throws[TokenInvalidException]
  @throws[PackageTooBigException]
  private final def methodFireAndForget[Req <: ThriftStruct](descriptor: Protocol.Descriptor[Req, _],
                                                             request: Req
                                                            ): Unit = {

    if (token == -1)
      scala.util.Try(Await.ready(authenticate(), clientOpts.requestTimeoutMs.millis)) match {
        case scala.util.Failure(_) => throw new TokenInvalidException()
        case _ => // do nothing.
      }

    val messageId = nextSeqId.getAndIncrement()
    val message = descriptor.encodeRequestToMessage(request)(messageId, token, isFireAndForgetMethod = true)

    if (logger.isDebugEnabled) logger.debug(Protocol.methodWithArgsToString(messageId, request))
    validateMessageSize(message)

    @tailrec
    def fire(): Unit = {
      if (channel.isActive)
        channel.writeAndFlush(message.toByteArray, channel.voidPromise())
      else {
        reconnect()
        fire()
      }
    }
    fire()
  }

  private def validateMessageSize(message: Message): Unit = {
    message.method match {
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

  private final def checkError(currentException: Throwable, previousException: Option[Throwable], retryCount: Int): (Throwable, Throwable, Int) = {
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
                  reconnect()
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

  /** Retrieving an offset between last processed commit log file and current commit log file where a server writes data.
    *
    * @return Future of getCommitLogOffsets operation that can be completed or not. If it is completed it returns:
    *         1)Thrift Struct [[com.bwsw.tstreamstransactionserver.rpc.CommitLogInfo]] which contains:
    *         currentProcessedCommitLog   - the one which is currently relayed with background worker
    *         currentConstructedCommitLog - the one which is currently under write routine
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server.
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path.
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getCommitLogOffsets(): ScalaFuture[com.bwsw.tstreamstransactionserver.rpc.CommitLogInfo] = {
    if (logger.isDebugEnabled()) logger.debug(s"Calling method 'getCommitLogOffsets' to get offsets.")
    onShutdownThrowException()
    method[TransactionService.GetCommitLogOffsets.Args, TransactionService.GetCommitLogOffsets.Result, com.bwsw.tstreamstransactionserver.rpc.CommitLogInfo](
      Protocol.GetCommitLogOffsets,
      TransactionService.GetCommitLogOffsets.Args(),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }


  /** Putting a stream on a server by primitive type parameters.
    *
    * @param stream      a name of stream.
    * @param partitions  a number of stream partitions.
    * @param description a description of stream.
    * @return Future of putStream operation that can be completed or not. If it is completed it returns:
    *         1) ID if stream is persisted by a server or -1 if there is a stream with such name on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path.
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         5) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): ScalaFuture[Int] = {
    if (logger.isDebugEnabled()) logger.debug(s"Putting stream $stream with $partitions partitions, ttl $ttl and description.")
    onShutdownThrowException()
    method[TransactionService.PutStream.Args, TransactionService.PutStream.Result, Int](
      Protocol.PutStream,
      TransactionService.PutStream.Args(stream, partitions, description, ttl),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Putting a stream on a server by Thrift Stream structure.
    *
    * @param stream an object of Thrift Stream [[com.bwsw.tstreamstransactionserver.rpc.StreamValue]] structure.
    * @return Future of putStream operation that can be completed or not. If it is completed it returns:
    *         1) ID if stream is persisted by a server or -1 if there is a stream with such name on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putStream(stream: com.bwsw.tstreamstransactionserver.rpc.StreamValue): ScalaFuture[Int] = {
    if (logger.isDebugEnabled()) logger.debug(s"Putting stream ${stream.name} with ${stream.partitions} partitions, ttl ${stream.ttl} and description.")
    onShutdownThrowException()

    method[TransactionService.PutStream.Args, TransactionService.PutStream.Result, Int](
      Protocol.PutStream,
      TransactionService.PutStream.Args(stream.name, stream.partitions, stream.description, stream.ttl),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Deleting a stream by name on a server.
    *
    * @param name a name of stream.
    * @return Future of putStream operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if stream is removed by a server or FALSE if a stream is already deleted or there in no such stream on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream name has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def delStream(name: String): ScalaFuture[Boolean] = {
    if (logger.isDebugEnabled) logger.debug(s"Deleting stream $name.")
    onShutdownThrowException()

    method[TransactionService.DelStream.Args, TransactionService.DelStream.Result, Boolean](
      Protocol.DelStream,
      TransactionService.DelStream.Args(name),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }


  /** Retrieving a stream from a server by it's name.
    *
    * @param name a name of stream.
    * @return Future of getStream operation that can be completed or not. If it is completed it returns:
    *         1) Thrift Stream [[com.bwsw.tstreamstransactionserver.rpc.StreamValue]] if stream  is retrieved from a server or throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]];
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getStream(name: String): ScalaFuture[Option[com.bwsw.tstreamstransactionserver.rpc.Stream]] = {
    if (logger.isDebugEnabled()) logger.debug(s"Retrieving stream $name.")
    onShutdownThrowException()

    method[TransactionService.GetStream.Args, TransactionService.GetStream.Result, Option[com.bwsw.tstreamstransactionserver.rpc.Stream]](
      Protocol.GetStream,
      TransactionService.GetStream.Args(name),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success
    )(context)
  }


  /** Checks by a stream's name that stream saved in database on server.
    *
    * @param name a name of stream.
    * @return Future of checkStreamExists operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if stream is exists in a server database or FALSE if a it's not;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream name has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def checkStreamExists(name: String): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info(s"Checking stream $name on existence...")
    onShutdownThrowException()

    method[TransactionService.CheckStreamExists.Args, TransactionService.CheckStreamExists.Result, Boolean](
      Protocol.CheckStreamExists,
      TransactionService.CheckStreamExists.Args(name),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** retrieving transaction id.
    *
    * @return Future of getTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Transaction ID
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def getTransaction(): ScalaFuture[Long] = {
    if (logger.isDebugEnabled())
      logger.debug(s"Retrieving transaction id ...")
    onShutdownThrowException()

    method[TransactionService.GetTransactionID.Args, TransactionService.GetTransactionID.Result, Long](
      Protocol.GetTransactionID,
      TransactionService.GetTransactionID.Args(),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }


  /** retrieving transaction id that is multiplied by timestamp
    *
    * @param timestamp multiplier(100000) to current transaction id.
    * @return Future of getTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Transaction ID
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def getTransaction(timestamp: Long): ScalaFuture[Long] = {
    if (logger.isDebugEnabled)
      logger.debug(s"Retrieving transaction id by timestamp $timestamp ...")
    onShutdownThrowException()

    method[TransactionService.GetTransactionIDByTimestamp.Args, TransactionService.GetTransactionIDByTimestamp.Result, Long](
      Protocol.GetTransactionIDByTimestamp,
      TransactionService.GetTransactionIDByTimestamp.Args(timestamp),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }


  /** Puts producer and consumer transactions on a server.
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
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def putTransactions(producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction],
                      consumerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction]): ScalaFuture[Boolean] = {
    val transactions =
      (producerTransactions map (txn => Transaction(Some(txn), None))) ++
        (consumerTransactions map (txn => Transaction(None, Some(txn))))

    if (logger.isDebugEnabled)
      logger.debug(s"putTransactions method is invoked: $transactions.")

    onShutdownThrowException()

    method[TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result, Boolean](
      Protocol.PutTransactions,
      TransactionService.PutTransactions.Args(transactions),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Puts producer transaction on a server.
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
  def putProducerState(transaction: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction): ScalaFuture[Boolean] = {
    if (logger.isDebugEnabled)
      logger.debug(s"Putting producer transaction ${transaction.transactionID} with state ${transaction.state} to stream ${transaction.stream}, partition ${transaction.partition}")
    val producerTransactionToTransaction = Transaction(Some(transaction), None)
    onShutdownThrowException()

    method[TransactionService.PutTransaction.Args, TransactionService.PutTransaction.Result, Boolean](
      Protocol.PutTransaction,
      TransactionService.PutTransaction.Args(producerTransactionToTransaction),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Puts consumer transaction on a server.
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
  def putTransaction(transaction: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    if (logger.isDebugEnabled()) logger.debug(s"Putting consumer transaction ${transaction.transactionID} with name ${transaction.name} to stream ${transaction.stream}, partition ${transaction.partition}")
    onShutdownThrowException()

    method[TransactionService.PutTransaction.Args, TransactionService.PutTransaction.Result, Boolean](
      Protocol.PutTransaction,
      TransactionService.PutTransaction.Args(Transaction(None, Some(transaction))),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Puts 'simplified' producer transaction that already has Chekpointed state with it's data
    *
    * @param streamID    an id of stream.
    * @param partition   a partition of stream.
    * @param data        a producer transaction data.
    * @return Future of putSimpleTransactionAndData operation that can be completed or not. If it is completed it returns:
    *         1) Transaction ID if transaction is persisted in commit log file for next processing and it's data is persisted successfully.
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putSimpleTransactionAndData(streamID: Int, partition: Int, data: Seq[Array[Byte]]): ScalaFuture[Long] = {
    if (logger.isDebugEnabled) logger.debug(s"Putting 'lightweight' producer transaction to stream $streamID, partition $partition with data: $data")
    onShutdownThrowException()

    method[TransactionService.PutSimpleTransactionAndData.Args, TransactionService.PutSimpleTransactionAndData.Result, Long](
      Protocol.PutSimpleTransactionAndData,
      TransactionService.PutSimpleTransactionAndData.Args(streamID, partition, data),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }


  /** Puts in fire and forget policy manner 'simplified' producer transaction that already has Chekpointed state with it's data.
    *
    * @param streamID    an id of stream.
    * @param partition   a partition of stream.
    * @param data        a producer transaction data.
    */
  def putSimpleTransactionAndDataWithoutResponse(streamID: Int, partition: Int, data: Seq[Array[Byte]]): Unit = {
    if (logger.isDebugEnabled) logger.debug(s"Putting 'lightweight' producer transaction to stream $streamID, partition $partition with data: $data")
    onShutdownThrowException()

    methodFireAndForget[TransactionService.PutSimpleTransactionAndData.Args](
      Protocol.PutSimpleTransactionAndData,
      TransactionService.PutSimpleTransactionAndData.Args(streamID, partition, data)
    )
  }


  /** Puts producer 'opened' transaction.
    *
    * @param streamID an id of stream.
    * @param partitionID  a partition of stream.
    * @param transactionTTLMs a lifetime of producer 'opened' transaction.
    * @return Future of openTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Transaction ID if transaction is persisted in commit log file for next processing.
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def openTransaction(streamID: Int, partitionID: Int, transactionTTLMs: Long): ScalaFuture[Long] = {
    if (logger.isDebugEnabled)
      logger.debug(s"Putting 'lightweight' producer transaction to stream $streamID, partition $partitionID with TTL: $transactionTTLMs")
    onShutdownThrowException()

    method[TransactionService.OpenTransaction.Args, TransactionService.OpenTransaction.Result, Long](
      Protocol.OpenTransaction,
      TransactionService.OpenTransaction.Args(streamID, partitionID, transactionTTLMs),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Retrieves a producer transaction by id
    *
    * @param streamID    an id of stream.
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
  def getTransaction(streamID: Int, partition: Int, transaction: Long): ScalaFuture[TransactionInfo] = {
    if (logger.isDebugEnabled()) logger.debug(s"Retrieving a producer transaction on partition '$partition' of stream '$streamID' by id '$transaction'")
    onShutdownThrowException()

    method[TransactionService.GetTransaction.Args, TransactionService.GetTransaction.Result, TransactionInfo](
      Protocol.GetTransaction,
      TransactionService.GetTransaction.Args(streamID, partition, transaction),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }


  /** Retrieves last checkpointed transaction in a specific stream on certain partition; If the result is -1 it will mean there is no checkpointed transaction at all.
    *
    * @param streamID  an id of stream.
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
  def getLastCheckpointedTransaction(streamID: Int, partition: Int): ScalaFuture[Long] = {
    if (logger.isDebugEnabled()) logger.debug(s"Retrieving a last checkpointed transaction on partition '$partition' of stream '$streamID")
    onShutdownThrowException()

    method[TransactionService.GetLastCheckpointedTransaction.Args, TransactionService.GetLastCheckpointedTransaction.Result, Long](
      Protocol.GetLastCheckpointedTransaction,
      TransactionService.GetLastCheckpointedTransaction.Args(streamID, partition),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }


  /** Retrieves all producer transactions in a specific range [from, to]; it's assumed that "from" and "to" are both positive.
    *
    * @param streamID    an id of stream.
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
  def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: Set[TransactionStates]): ScalaFuture[ScanTransactionsInfo] = {
    require(from >= 0 && to >= 0, s"Calling method scanTransactions requires that bounds: 'from' and 'to' are both positive(actually from and to are: [$from, $to])")
    if (to < from || count == 0) {
      onShutdownThrowException()
      val lastOpenedTransactionID = -1L
      ScalaFuture.successful(ScanTransactionsInfo(lastOpenedTransactionID, collection.immutable.Seq()))
    }
    else {
      if (logger.isDebugEnabled()) logger.debug(s"Retrieving producer transactions on stream $streamID in range [$from, $to]")
      onShutdownThrowException()

      method[TransactionService.ScanTransactions.Args, TransactionService.ScanTransactions.Result, ScanTransactionsInfo](
        Protocol.ScanTransactions,
        TransactionService.ScanTransactions.Args(streamID, partition, from, to, count, states),
        x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
      )(context)
    }
  }


  /** Putting any binary data on server to a specific stream, partition, transaction id of producer transaction.
    *
    * @param streamID    an id of stream.
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
  def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    if (logger.isDebugEnabled)
      logger.debug(s"Putting transaction data to stream $streamID, partition $partition, transaction $transaction.")
    onShutdownThrowException()

    method[TransactionService.PutTransactionData.Args, TransactionService.PutTransactionData.Result, Boolean](
      Protocol.PutTransactionData,
      TransactionService.PutTransactionData.Args(streamID, partition, transaction, data, from),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Putting any binary data and persisting/updating transaction state on server.
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
  def putProducerStateWithData(producerTransaction: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = ScalaFuture {
    import producerTransaction._
    if (logger.isDebugEnabled)
      logger.debug(
        s"Putting producer transaction to stream " +
          s"$stream, partition $partition, transaction $transactionID, state $state, ttl: $ttl, quantity: $quantity " +
          s"with data $data"
      )

    onShutdownThrowException()

    method[TransactionService.PutProducerStateWithData.Args, TransactionService.PutProducerStateWithData.Result, Boolean](
      Protocol.PutProducerStateWithData,
      TransactionService.PutProducerStateWithData.Args(producerTransaction, data, from),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }(contextForProducerTransactions).flatten


  /** Retrieves all producer transactions binary data in a specific range [from, to]; it's assumed that "from" and "to" are both positive.
    *
    * @param streamID    an id of stream
    * @param partition   a partition of stream.
    * @param transaction a transaction id.
    * @param from        an inclusive bound to strat with.
    * @param to          an inclusive bound to end with.
    * @return Future of getTransactionData operation that can be completed or not. If it is completed it returns:
    *         1) Collection of binary data in range [from, to] on certain stream on certain partition if data presents.
    *         otherwise FALSE(operation isn't atomic, it's split into 2 operations: first putting producer state, then its data);
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0, s"Calling method getTransactionData requires that bounds: 'from' and 'to' are both positive(actually from and to are: [$from, $to])")
    if (to < from) {
      onShutdownThrowException()
      ScalaFuture.successful(Seq[Array[Byte]]())
    }
    else {
      if (logger.isDebugEnabled) logger.debug(s"Retrieving producer transaction data from stream $streamID, partition $partition, transaction $transaction in range [$from, $to].")
      onShutdownThrowException()

      method[TransactionService.GetTransactionData.Args, TransactionService.GetTransactionData.Result, Seq[Array[Byte]]](
        Protocol.GetTransactionData,
        TransactionService.GetTransactionData.Args(streamID, partition, transaction, from, to),
        x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
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
  def putConsumerCheckpoint(consumerTransaction: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    onShutdownThrowException()
    if (logger.isDebugEnabled())
      logger.debug(s"Setting consumer state ${consumerTransaction.name} on stream ${consumerTransaction.stream}, partition ${consumerTransaction.partition}, transaction ${consumerTransaction.transactionID}.")

    method[TransactionService.PutConsumerCheckpoint.Args, TransactionService.PutConsumerCheckpoint.Result, Boolean](
      Protocol.PutConsumerCheckpoint,
      TransactionService.PutConsumerCheckpoint.Args(consumerTransaction.name, consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Retrieves a consumer state on a specific consumer transaction name, stream, partition from a server; If the result is -1 it will mean there is no checkpoint at all.
    *
    * @param name      a consumer transaction name.
    * @param streamID  an id of stream.
    * @param partition a partition of the stream.
    * @return Future of getConsumerState operation that can be completed or not. If it is completed it returns:
    *         1) transaction ID on certain stream on certatin partition if it exists, otherwise -1L
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getConsumerState(name: String, streamID: Int, partition: Int): ScalaFuture[Long] = {
    if (logger.isDebugEnabled) logger.debug(s"Retrieving a transaction by consumer $name on stream $streamID, partition $partition.")
    onShutdownThrowException()

    method[TransactionService.GetConsumerState.Args, TransactionService.GetConsumerState.Result, Long](
      Protocol.GetConsumerState,
      TransactionService.GetConsumerState.Args(name, streamID, partition),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
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
  private def authenticate(): ScalaFuture[Unit] = {
    val isAuth = isAuthenticated.getAndSet(true)
    if (!isAuth) {
      if (logger.isInfoEnabled) logger.info("authenticate method is invoked.")
      onShutdownThrowException()
      val authKey = authOpts.key

      method[TransactionService.Authenticate.Args, TransactionService.Authenticate.Result, Unit](
        Protocol.Authenticate,
        TransactionService.Authenticate.Args(authKey),
        x => {
          val authInfo = x.success.get
          token = authInfo.token
          maxDataPackageSize = authInfo.maxDataPackageSize
          maxMetadataPackageSize = authInfo.maxMetadataPackageSize
          isAuthenticated.set(false)
        }
      )(context)
    } else {
      TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
      ScalaFuture.successful[Unit](Unit)
    }
  }

  /** It Disconnects client from server slightly */
  def shutdown(): Unit = {
    if (!isShutdown) {
      isShutdown = true
      if (workerGroup != null) {
        workerGroup.shutdownGracefully(0, 0, TimeUnit.NANOSECONDS)
      }
      if (channel != null) channel.closeFuture().sync()
      if (zkInteractor != null) zkInteractor.close()
      processTransactionsPutOperationPool.stopAccessNewTasks()
      processTransactionsPutOperationPool.awaitAllCurrentTasksAreCompleted()
      executionContext.stopAccessNewTasksAndAwaitCurrentTasksToBeCompleted()
    }
  }
}