package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits._
import com.bwsw.tstreamstransactionserver.configProperties.ClientExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwables
import com.bwsw.tstreamstransactionserver.exception.Throwables.{ServerConnectionException, ServerUnreachableException, TokenInvalidException, ZkGetMasterException}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, ExecutionContext}
import com.bwsw.tstreamstransactionserver.options.{AuthOptions, ClientOptions, ZookeeperOptions}
import com.bwsw.tstreamstransactionserver.zooKeeper.ZKLeaderClientToGetMaster
import com.twitter.scrooge.ThriftStruct
import io.netty.bootstrap.Bootstrap
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ChannelOption}
import org.apache.curator.retry.RetryNTimes
import org.slf4j.LoggerFactory
import transactionService.rpc.{TransactionService, _}

import scala.annotation.tailrec
import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise}


/** A client who connects to a server.
  *
  * @constructor create a new client by configuration file or map.
  */
class Client(clientOpts: ClientOptions, authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val executionContext = new ClientExecutionContext(clientOpts.threadPool)

  val zKLeaderClient = new ZKLeaderClientToGetMaster(zookeeperOpts.endpoints,
    zookeeperOpts.sessionTimeoutMs, zookeeperOpts.connectionTimeoutMs,
    new RetryNTimes(zookeeperOpts.retryCount, zookeeperOpts.retryDelayMs), zookeeperOpts.prefix)
  zKLeaderClient.start()

  private implicit final val context = executionContext.context

  private val nextSeqId = new AtomicInteger(Int.MinValue)
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
  @tailrec
  private def getInetAddressFromZookeeper(times: Int): (String, Int) = {
    if (times > 0 && zKLeaderClient.master.isEmpty) {
      TimeUnit.MILLISECONDS.sleep(zookeeperOpts.retryDelayMs)
      logger.info("Retrying to get master server from zookeeper server.")
      getInetAddressFromZookeeper(times - 1)
    } else {
      zKLeaderClient.master match {
        case Some(master) => val listenPort = master.split(":")
          (listenPort(0), listenPort(1).toInt)
        case None => {
          logger.error(Throwables.zkGetMasterExceptionMessage)
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

  private def retryMethod[Req, Rep](f: => ScalaFuture[Rep]) = retry(authOpts.connectionTimeoutMs / authOpts.retryDelayMs)(f)(conditionToRetry)

  private val conditionToRetry = new PartialFunction[Throwable, Boolean] {
    override def apply(v1: Throwable): Boolean = {
      v1 match {
        case _: TokenInvalidException =>
          logger.info("Token isn't valid. Retrying get one.")
          authenticate()
          TimeUnit.MILLISECONDS.sleep(authOpts.tokenRetryDelayMs)
          true
        case _: ServerUnreachableException =>
          logger.info(s"${Throwables.serverUnreachableExceptionMessage}. Retrying to reconnect server.")
          TimeUnit.MILLISECONDS.sleep(authOpts.retryDelayMs)
          true
        case error =>
          logger.error(error.getMessage, error)
          false
      }
    }

    override def isDefinedAt(x: Throwable): Boolean = x.isInstanceOf[TokenInvalidException]
  }

  @volatile private var token: Int = _
  //Await.ready(authenticate(), 5 seconds)

  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Int): ScalaFuture[Boolean] = {
    logger.info("PutStream method is invoked.")
    retryMethod(method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream, partitions, description, ttl))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  def putStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    logger.info("PutStream method is invoked.")
    retryMethod(method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream.name, stream.partitions, stream.description, stream.ttl))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  def delStream(stream: String): ScalaFuture[Boolean] = {
    logger.info("delStream method is invoked.")
    retryMethod(method(Descriptors.DelStream, TransactionService.DelStream.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  def delStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    logger.info("delStream method is invoked.")
    retryMethod(method(Descriptors.DelStream, TransactionService.DelStream.Args(token, stream.name))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  def getStream(stream: String): ScalaFuture[transactionService.rpc.Stream] = {
    logger.info("getStream method is invoked.")
    retryMethod(method(Descriptors.GetStream, TransactionService.GetStream.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  def doesStreamExist(stream: String): ScalaFuture[Boolean] = {
    logger.info("doesStreamExist method is invoked.")
    retryMethod(method(Descriptors.DoesStreamExist, TransactionService.DoesStreamExist.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  private final val futurePool = ExecutionContext(1, "ClientTransactionPool-%d").getContext

  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): ScalaFuture[Boolean] = {
    logger.info("putTransactions method is invoked.")

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


  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): ScalaFuture[Boolean] = {
    logger.info("putTransaction method is invoked.")
    TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None))
    retryMethod(method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None)))(futurePool)
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool))
  }


  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    logger.info("putTransaction method is invoked.")
    retryMethod(method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(None, Some(transaction))))(futurePool)
      .flatMap(x => /*if (x.error.isDefined) ScalaFuture.failed(com.bwsw.exception.Throwables.byText(x.error.get.message)) else*/ ScalaFuture.successful(true /*x.success.get*/))(futurePool))
  }


  def scanTransactions(stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[transactionService.rpc.ProducerTransaction]] = {
    logger.info("scanTransactions method is invoked.")
    retryMethod(method(Descriptors.ScanTransactions, TransactionService.ScanTransactions.Args(token, stream, partition, from, to))
      .flatMap(x =>
        if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message))
        else
          ScalaFuture.successful(x.success.get.withFilter(_.consumerTransaction.isEmpty).map(_.producerTransaction.get))
      )
    )
  }


  def putTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    logger.info("putTransactionData method is invoked.")
    retryMethod(method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, producerTransaction.stream,
      producerTransaction.partition, producerTransaction.transactionID, data, from))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  def putTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    logger.info("putTransactionData method is invoked.")
    retryMethod(method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, consumerTransaction.stream,
      consumerTransaction.partition, consumerTransaction.transactionID, data, from))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  def getTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)
    logger.info("getTransactionData method is invoked.")

    retryMethod(method(Descriptors.GetTransactionData, TransactionService.GetTransactionData.Args(token, producerTransaction.stream,
      producerTransaction.partition, producerTransaction.transactionID, from, to))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  def getTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)
    logger.info("getTransactionData method is invoked.")

    retryMethod(method(Descriptors.GetTransactionData, TransactionService.GetTransactionData.Args(token, consumerTransaction.stream,
      consumerTransaction.partition, consumerTransaction.transactionID, from, to))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  def setConsumerState(consumerTransaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    logger.info("setConsumerState method is invoked.")

    retryMethod(method(Descriptors.SetConsumerState, TransactionService.SetConsumerState.Args(token, consumerTransaction.name,
      consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool)
    )
  }

  def getConsumerState(consumerTransaction: (String, String, Int)): ScalaFuture[Long] = {
    logger.info("getConsumerState method is invoked.")
    retryMethod(method(Descriptors.GetConsumerState, TransactionService.GetConsumerState.Args(token, consumerTransaction._1, consumerTransaction._2, consumerTransaction._3))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  private def authenticate(): ScalaFuture[Unit] = {
    logger.info("authenticate method is invoked.")
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