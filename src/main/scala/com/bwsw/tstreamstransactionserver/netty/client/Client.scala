package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits._
import com.bwsw.tstreamstransactionserver.configProperties.ClientExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwables
import com.bwsw.tstreamstransactionserver.exception.Throwables.{ServerConnectionException, ServerUnreachableException, TokenInvalidException, ZkGetMasterException}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, ExecutionContext}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.zooKeeper.ZKLeaderClientToGetMaster
import com.twitter.scrooge.ThriftStruct
import io.netty.bootstrap.Bootstrap
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ChannelOption}
import org.apache.curator.retry.RetryForever
import org.slf4j.LoggerFactory
import transactionService.rpc.{TransactionService, _}

import scala.annotation.tailrec
import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise}


/** A client who connects to a server.
  *
  * @constructor create a new client by configuration file or map.
  */
class Client(clientOpts: ConnectionOptions, authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val executionContext = new ClientExecutionContext(clientOpts.threadPool)

  val zKLeaderClient = new ZKLeaderClientToGetMaster(zookeeperOpts.endpoints,
    zookeeperOpts.sessionTimeoutMs, zookeeperOpts.connectionTimeoutMs,
    new RetryForever(zookeeperOpts.retryDelayMs), zookeeperOpts.prefix)
  zKLeaderClient.start()

  private implicit final val context = executionContext.context

  private val nextSeqId = new AtomicInteger(1)
  private val ReqIdToRep = new ConcurrentHashMap[Int, ScalaPromise[ThriftStruct]](10000, 1.0f, clientOpts.threadPool)
  private val workerGroup = new EpollEventLoopGroup()

  private val bootstrap = new Bootstrap()
    .group(workerGroup)
    .channel(classOf[EpollSocketChannel])
    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    .handler(new ClientInitializer(ReqIdToRep, this, context))


  @volatile private var channel: Channel = {
    connect()
    null
  }

  def connect(): Unit = {
    val (listen, port) = getInetAddressFromZookeeper(zookeeperOpts.connectionTimeoutMs / zookeeperOpts.retryDelayMs)
    bootstrap.connect(listen, port).addListener(new ConnectionListener)
  }

  private class ConnectionListener extends ChannelFutureListener() {
    val atomicInteger = new AtomicInteger(clientOpts.connectionTimeoutMs / clientOpts.retryDelayMs)

    @throws[Exception]
    override def operationComplete(channelFuture: ChannelFuture): Unit = {
      if (!channelFuture.isSuccess && atomicInteger.getAndDecrement() > 0) {
        //      System.out.println("Reconnect")
        val loop = channelFuture.channel().eventLoop()
        loop.execute(() => {
          TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
          connect()
        })
      } else if (atomicInteger.get() <= 0) throw new ServerConnectionException
      else channel = channelFuture.sync().channel()
    }
  }


  /** A retry method to get ipAddres:port from zooKeeper server.
    *
    * @param times how many times try to get ipAddres:port from zooKeeper server.
    */
  @tailrec @throws[ZkGetMasterException]
  private def getInetAddressFromZookeeper(times: Int): (String, Int) = {
    if (times > 0 && zKLeaderClient.master.isEmpty) {
      TimeUnit.MILLISECONDS.sleep(zookeeperOpts.retryDelayMs)
      if (logger.isInfoEnabled) logger.info("Retrying to get master server from zookeeper server.")
      getInetAddressFromZookeeper(times - 1)
    } else {
      zKLeaderClient.master match {
        case Some(master) => val listenPort = master.split(":")
          (listenPort(0), listenPort(1).toInt)
        case None => {
          if (logger.isErrorEnabled) logger.error(Throwables.zkGetMasterExceptionMessage)
          shutdown()
          throw new ZkGetMasterException
        }
      }
    }
  }

  /** A general method for sending requests to a server and getting a response back.
    *
    * @param descriptor .
    * @param request
    */

  private def method[Req <: ThriftStruct, Rep <: ThriftStruct](descriptor: Descriptors.Descriptor[Req, Rep], request: Req)
                                                              (implicit context: concurrent.ExecutionContext): ScalaFuture[Rep] = {
    if (channel != null && channel.isActive) {
      val messageId = nextSeqId.getAndIncrement()
      val promise = ScalaPromise[ThriftStruct]
      val message = descriptor.encodeRequest(request)(messageId)
      ReqIdToRep.put(messageId, promise)
      channel.writeAndFlush(message.toByteArray)
      promise.future.map { response =>
        ReqIdToRep.remove(messageId)
        response.asInstanceOf[Rep]
      }.recoverWith { case error =>
        ReqIdToRep.remove(messageId)
        ScalaFuture.failed(error)
      }
    } else ScalaFuture.failed(new ServerUnreachableException)
  }

  private def retry[Req, Rep](times: Int)(f: => ScalaFuture[Rep])(condition: PartialFunction[Throwable, Boolean]): ScalaFuture[Rep] = {
    def helper(times: Int)(f: => ScalaFuture[Rep]): ScalaFuture[Rep] = f.recoverWith {
      case error if times > 0 && condition(error) =>
        helper(times - 1)(f)
    }

    helper(times)(f)
  }

  private def retryMethod[Req, Rep](f: => ScalaFuture[Rep]) = retry(clientOpts.connectionTimeoutMs / clientOpts.retryDelayMs)(f)(conditionToRetry)

  private val conditionToRetry = new PartialFunction[Throwable, Boolean] {
    override def apply(throwable: Throwable): Boolean = {
      throwable match {
        case _: TokenInvalidException =>
          if (logger.isInfoEnabled) logger.info("Token isn't valid. Retrying get one.")
          authenticate()
          TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
          true
        case _: ServerUnreachableException =>
          if (logger.isInfoEnabled) logger.info(s"${Throwables.serverUnreachableExceptionMessage}. Retrying to reconnect server.")
          TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
          true
        case error =>
          if (logger.isErrorEnabled) logger.error(error.getMessage, error)
          false
      }
    }

    override def isDefinedAt(x: Throwable): Boolean = x.isInstanceOf[TokenInvalidException]
  }

  @volatile private var token: Int = _

  /** Putting a stream on a server by primitive type parameters.
    *
    * @param stream a name of stream.
    * @param partitions a number of stream partitions.
    * @param description a description of stream.
    *
    * @return placeholder of putStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Int): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("PutStream method is invoked.")
    retryMethod(method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream, partitions, description, ttl))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  /**  Putting a stream on a server by Thrift Stream structure.
    *
    * @param stream an object of Stream structure.
    *
    * @return placeholder of putStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def putStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("PutStream method is invoked.")
    retryMethod(method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream.name, stream.partitions, stream.description, stream.ttl))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  /** Deleting a stream by name on a server.
    *
    * @param stream a name of stream.
    *
    * @return placeholder of delStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def delStream(stream: String): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("delStream method is invoked.")
    retryMethod(method(Descriptors.DelStream, TransactionService.DelStream.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  /** Deleting a stream by Thrift Stream structure on a server.
    *
    * @param stream a name of stream.
    *
    * @return placeholder of delStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def delStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("delStream method is invoked.")
    retryMethod(method(Descriptors.DelStream, TransactionService.DelStream.Args(token, stream.name))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Retrieving a stream from a server by it's name.
    *
    * @param stream a name of stream.
    *
    * @return placeholder of getStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def getStream(stream: String): ScalaFuture[transactionService.rpc.Stream] = {
    if (logger.isInfoEnabled) logger.info("getStream method is invoked.")
    retryMethod(method(Descriptors.GetStream, TransactionService.GetStream.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  /** Checks by a stream's name that stream saved in database on server.
    *
    * @param stream a name of stream.
    *
    * @return placeholder of doesStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def checkStreamExists(stream: String): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("doesStreamExist method is invoked.")
    retryMethod(method(Descriptors.CheckStreamExists, TransactionService.CheckStreamExists.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** A special context for making requests asynchronously, although they are processed sequentially;
    * If's for: putTransaction, putTransactions, setConsumerState.
    */
  private final val futurePool = ExecutionContext(1, "ClientTransactionPool-%d").getContext


  /** Puts producer and consumer transactions on a server; it's implied there were persisted streams on a server transactions belong to, otherwise
    * the exception would be thrown.
    *
    * @param producerTransactions some collections of producer transactions.
    * @param consumerTransactions some collections of consumer transactions.
    *
    * @return placeholder of putTransactions operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("putTransactions method is invoked.")

    val txns = (producerTransactions map (txn => Transaction(Some(txn), None))) ++
      (consumerTransactions map (txn => Transaction(None, Some(txn))))

    retryMethod(method(Descriptors.PutTransactions, TransactionService.PutTransactions.Args(token, txns))(futurePool)
      .flatMap(x => if (x.error.isDefined) {
        ScalaFuture.failed(Throwables.byText(x.error.get.message))
      } else {
        ScalaFuture.successful(x.success.get)
      })(futurePool)
    )
  }

  /** Puts producer transaction on a server; it's implied there was persisted stream on a server transaction belong to, otherwise
    * the exception would be thrown.
    *
    * @param transaction a producer transactions.
    *
    * @return placeholder of putTransaction operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("putTransaction method is invoked.")
    TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None))
    retryMethod(method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None)))(futurePool)
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool))
  }


  /** Puts consumer transaction on a server; it's implied there was persisted stream on a server transaction belong to, otherwise
    * the exception would be thrown.
    *
    * @param transaction a consumer transactions.
    *
    * @return placeholder of putTransaction operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("putTransaction method is invoked.")
    retryMethod(method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(None, Some(transaction))))(futurePool)
      .flatMap(x => /*if (x.error.isDefined) ScalaFuture.failed(com.bwsw.exception.Throwables.byText(x.error.get.message)) else*/ ScalaFuture.successful(true /*x.success.get*/))(futurePool))
  }

  /** Retrives all producer tranasactions in a specific range [from; to); it's assumed that from >= to and they are both positive.
    *
    *
    * @param stream a name of stream.
    * @param partition a partition of stream.
    * @param from an inclusive bound to strat with.
    * @param to an exclusive bound to end with.
    *
    * @return placeholder of putTransaction operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def scanTransactions(stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[transactionService.rpc.ProducerTransaction]] = {
    require(from >= 0 && to >= 0 && to >= from)
    if (logger.isInfoEnabled) logger.info("scanTransactions method is invoked.")
    retryMethod(method(Descriptors.ScanTransactions, TransactionService.ScanTransactions.Args(token, stream, partition, from, to))
      .flatMap(x =>
        if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message))
        else
          ScalaFuture.successful(x.success.get.withFilter(_.consumerTransaction.isEmpty).map(_.producerTransaction.get))
      )
    )
  }

  /** Putting any binary data on server to a specific stream, partition, transaction id of producer tranasaction.
    *
    *
    * @param stream a stream.
    * @param partition a partition of stream.
    * @param transaction a transaction ID.
    * @param data a data to persist.
    * @param from an inclusive bound to strat with.
    *
    * @return placeholder of putTransaction operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def putTransactionData(stream: String, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int) = {
    if (logger.isInfoEnabled) logger.info("putTransactionData method is invoked.")
    retryMethod(method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, stream, partition, transaction, data, from))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Putting any binary data and setting transaction state on server.
    *
    *
    * @param producerTransaction a producer transaction contains all necessary information for persisting data.
    * @param data a data to persist.
    *
    * @return placeholder of putTransactionData operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def putTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    putTransaction(producerTransaction) flatMap {response =>
      if (logger.isInfoEnabled) logger.info("putTransactionData method is invoked.")
      retryMethod(method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, producerTransaction.stream,
        producerTransaction.partition, producerTransaction.transactionID, data, from))
        .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
      )
    }
  }


  /** Retrieves all producer transactions binary data in a specific range [from; to]; it's assumed that from >= to and they are both positive.
    *
    *
    * @param stream a stream
    * @param partition a partition of stream.
    * @param transaction a transaction id.
    * @param from an inclusive bound to strat with.
    * @param to an inclusive bound to end with.
    *
    * @return placeholder of getTransactionData operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def getTransactionData(stream: String, partition: Int, transaction: Long, from: Int, to: Int) = {
    require(from >= 0 && to >= 0 && to >= from)
    if (logger.isInfoEnabled) logger.info("getTransactionData method is invoked.")

    retryMethod(method(Descriptors.GetTransactionData, TransactionService.GetTransactionData.Args(token, stream, partition, transaction, from, to))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(byteBuffersToSeqArrayByte(x.success.get)))
    )
  }


  /** Puts/Updates a consumer state on a specific stream, partition, transaction id on a server.
    *
    *
    * @param consumerTransaction a consumer transaction contains all necessary information for putting/updating it's state.
    *
    * @return placeholder of setConsumerState operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def setConsumerState(consumerTransaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("setConsumerState method is invoked.")

    retryMethod(method(Descriptors.SetConsumerState, TransactionService.SetConsumerState.Args(token, consumerTransaction.name,
      consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool)
    )
  }

  /** Retrieves a consumer state on a specific consumer transaction name, stream, partition from a server.
    *
    * @param name a consumer transaction name.
    * @param stream a stream.
    * @param partition a partition of the stream.
    *
    * @return placeholder of getConsumerState operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  def getConsumerState(name: String, stream: String, partition: Int): ScalaFuture[Long] = {
    if (logger.isInfoEnabled) logger.info("getConsumerState method is invoked.")
    retryMethod(method(Descriptors.GetConsumerState, TransactionService.GetConsumerState.Args(token, name, stream, partition))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Retrieves a token for that allow a client send requests to server.
    *
    * @return placeholder of authenticate operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  private def authenticate(): ScalaFuture[Unit] = {
    if (logger.isInfoEnabled) logger.info("authenticate method is invoked.")
    val authKey = authOpts.key
    method(Descriptors.Authenticate, TransactionService.Authenticate.Args(authKey))
      .map(x => token = x.success.get)
  }

  def shutdown() = {
    zKLeaderClient.close()
    workerGroup.shutdownGracefully()
    if (channel != null) channel.closeFuture().sync()
    executionContext.shutdown()
  }
}