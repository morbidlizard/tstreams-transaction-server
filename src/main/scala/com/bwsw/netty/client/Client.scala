package com.bwsw.netty.client

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.bwsw.`implicit`.Implicits._
import com.bwsw.exception.Throwables
import com.twitter.scrooge.ThriftStruct
import com.bwsw.exception.Throwables.{ServerConnectionException, ZkGetMasterException}
import com.bwsw.netty.{Context, Descriptors}
import org.apache.curator.retry.RetryNTimes
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import transactionService.rpc.{TransactionService, _}
import com.bwsw.zooKeeper.ZKLeaderClientToGetMaster
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ChannelOption}
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}

class Client(config: com.bwsw.configProperties.ClientConfig = new com.bwsw.configProperties.ClientConfig(new com.bwsw.configProperties.ConfigFile("src/main/resources/clientProperties.properties"))) {
  import config._

  PropertyConfigurator.configure("src/main/resources/logClient.properties")
  private val logger = LoggerFactory.getLogger(classOf[Client])

  val zKLeaderClient = new ZKLeaderClientToGetMaster(zkEndpoints, zkTimeoutSession, zkTimeoutConnection,
    new RetryNTimes(zkRetriesMax, zkTimeoutBetweenRetries), zkPrefix)
  zKLeaderClient.start()

  private implicit final val context = clientPoolContext

  private val nextSeqId = new AtomicInteger(Int.MinValue)
  private val ReqIdToRep = new ConcurrentHashMap[Int, ScalaPromise[ThriftStruct]](10000, 1.0f, clientPool)
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
    val (listen, port) = getInetAddressFromZookeeper(zkTimeoutConnection / zkTimeoutBetweenRetries)
    bootstrap.connect(listen, port).addListener(new ConnectionListener)
  }

  private class ConnectionListener extends ChannelFutureListener() {
    val atomicInteger = new AtomicInteger(serverTimeoutConnection / serverTimeoutBetweenRetries)

    @throws[Exception]
    override def operationComplete(channelFuture: ChannelFuture): Unit = {
      if (!channelFuture.isSuccess && atomicInteger.getAndDecrement() > 0) {
        //      System.out.println("Reconnect")
        val loop = channelFuture.channel().eventLoop()
        loop.execute(new Runnable() {
          override def run() {
            TimeUnit.MILLISECONDS.sleep(serverTimeoutBetweenRetries)
            connect()
          }
        })
      } else if (atomicInteger.get() <= 0) throw new ServerConnectionException
      else channel = channelFuture.sync().channel()
    }
  }


  @tailrec
  final def getInetAddressFromZookeeper(times: Int): (String, Int) = {
    if (times > 0 && zKLeaderClient.master.isEmpty) {
      TimeUnit.MILLISECONDS.sleep(zkTimeoutBetweenRetries)
      logger.info("Retrying to get master server from zookeeper server.")
      getInetAddressFromZookeeper(times - 1)
    } else {
      zKLeaderClient.master match {
        case Some(master) => val listenPort = master.split(":")
          (listenPort(0), listenPort(1).toInt)
        case None => {
          logger.error(Throwables.zkGetMasterExceptionMessage)
          throw new ZkGetMasterException
        }
      }
    }
  }

  private def method[Req <: ThriftStruct, Rep <: ThriftStruct](descriptor: Descriptors.Descriptor[Req, Rep], request: Req)(implicit context: ExecutionContext): ScalaFuture[Rep] = {
    if (channel != null && channel.isActive) {
      val messageId = nextSeqId.getAndIncrement()
      val promise = ScalaPromise[ThriftStruct]
      val message = descriptor.encodeRequest(request)(messageId)
      ReqIdToRep.put(messageId, promise)
      channel.writeAndFlush(message.toByteArray)
      promise.future.map { response =>
        ReqIdToRep.remove(messageId)
        response.asInstanceOf[Rep]
      }.recover{case error => ReqIdToRep.remove(messageId); throw error}
    } else ScalaFuture.failed(new com.bwsw.exception.Throwables.ServerUnreachableException)
  }

  private def retry[Req, Rep](times: Int)(f: => ScalaFuture[Rep])(condition: PartialFunction[Throwable, Boolean]): ScalaFuture[Rep] = {
    def helper(times: Int)(f: => ScalaFuture[Rep]): ScalaFuture[Rep] = f.recoverWith {
      case error if times > 0 && condition(error) =>
        helper(times - 1)(f)
    }

    helper(times)(f)
  }

  private def retryAuthenticate[Req, Rep](f: => ScalaFuture[Rep]) = retry(authTimeoutConnection / authTokenTimeoutBetweenRetries)(f)(
    new PartialFunction[Throwable, Boolean] {
      override def apply(v1: Throwable): Boolean = v1 match {
        case _: com.bwsw.exception.Throwables.TokenInvalidException =>
          logger.info("Token isn't valid. Retrying get one.")
          authenticate()
          TimeUnit.MILLISECONDS.sleep(authTokenTimeoutBetweenRetries)
          true
        case _: com.bwsw.exception.Throwables.ServerUnreachableException =>
          logger.info(s"${Throwables.serverUnreachableExceptionMessage}Retrying to reconnect server.")
          TimeUnit.MILLISECONDS.sleep(authTokenTimeoutBetweenRetries)
          true
        case error =>
          logger.error(error.getMessage, error)
          false
      }

      override def isDefinedAt(x: Throwable): Boolean = x.isInstanceOf[com.bwsw.exception.Throwables.TokenInvalidException]
    }
  )

  @volatile private var token: Int = _
  //Await.ready(authenticate(), 5 seconds)

  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Int): ScalaFuture[Boolean] = {
    logger.info("PutStream method is invoked.")
    retryAuthenticate(method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream, partitions, description, ttl))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  def putStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    logger.info("PutStream method is invoked.")
    retryAuthenticate(method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream.name, stream.partitions, stream.description, stream.ttl))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  def delStream(stream: String): ScalaFuture[Boolean] = {
    logger.info("delStream method is invoked.")
    retryAuthenticate(method(Descriptors.DelStream, TransactionService.DelStream.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  def delStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    logger.info("delStream method is invoked.")
    retryAuthenticate(method(Descriptors.DelStream, TransactionService.DelStream.Args(token, stream.name))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  def getStream(stream: String): ScalaFuture[transactionService.rpc.Stream] = {
    logger.info("getStream method is invoked.")
    retryAuthenticate(method(Descriptors.GetStream, TransactionService.GetStream.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  def doesStreamExist(stream: String): ScalaFuture[Boolean] = {
    logger.info("doesStreamExist method is invoked.")
    retryAuthenticate(method(Descriptors.DoesStreamExist, TransactionService.DoesStreamExist.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  private final val futurePool = Context(1, "ClientTransactionPool-%d").getContext

  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): ScalaFuture[Boolean] = {
    logger.info("putTransactions method is invoked.")

    val txns = (producerTransactions map (txn => Transaction(Some(txn), None))) ++
      (consumerTransactions map (txn => Transaction(None, Some(txn))))

    retryAuthenticate(method(Descriptors.PutTransactions, TransactionService.PutTransactions.Args(token, txns))(futurePool)
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool)
    )
  }


  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): ScalaFuture[Boolean] = {
    logger.info("putTransaction method is invoked.")
    TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None))
    retryAuthenticate(method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None)))(futurePool)
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool))
  }


  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    logger.info("putTransaction method is invoked.")
    retryAuthenticate(method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(None, Some(transaction))))(futurePool)
      .flatMap(x => /*if (x.error.isDefined) ScalaFuture.failed(com.bwsw.exception.Throwables.byText(x.error.get.message)) else*/ ScalaFuture.successful(true /*x.success.get*/))(futurePool))
  }


  def scanTransactions(stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[transactionService.rpc.ProducerTransaction]] = {
    logger.info("scanTransactions method is invoked.")
    retryAuthenticate(method(Descriptors.ScanTransactions, TransactionService.ScanTransactions.Args(token, stream, partition, from, to))
      .flatMap(x =>
        if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message))
        else
          ScalaFuture.successful(x.success.get.withFilter(_.consumerTransaction.isEmpty).map(_.producerTransaction.get))
      )
    )
  }


  def putTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    logger.info("putTransactionData method is invoked.")
    retryAuthenticate(method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, producerTransaction.stream,
      producerTransaction.partition, producerTransaction.transactionID, data, from))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  def putTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    logger.info("putTransactionData method is invoked.")
    retryAuthenticate(method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, consumerTransaction.stream,
      consumerTransaction.partition, consumerTransaction.transactionID, data, from))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  def getTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)
    logger.info("getTransactionData method is invoked.")

    retryAuthenticate(method(Descriptors.GetTransactionData, TransactionService.GetTransactionData.Args(token, producerTransaction.stream,
      producerTransaction.partition, producerTransaction.transactionID, from, to))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  def getTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)
    logger.info("getTransactionData method is invoked.")

    retryAuthenticate(method(Descriptors.GetTransactionData, TransactionService.GetTransactionData.Args(token, consumerTransaction.stream,
      consumerTransaction.partition, consumerTransaction.transactionID, from, to))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  def setConsumerState(consumerTransaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    logger.info("setConsumerState method is invoked.")

    retryAuthenticate(method(Descriptors.SetConsumerState, TransactionService.SetConsumerState.Args(token, consumerTransaction.name,
      consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool)
    )
  }

  def getConsumerState(consumerTransaction: (String, String, Int)): ScalaFuture[Long] = {
    logger.info("getConsumerState method is invoked.")
    retryAuthenticate(method(Descriptors.GetConsumerState, TransactionService.GetConsumerState.Args(token, consumerTransaction._1, consumerTransaction._2, consumerTransaction._3))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  private def authenticate(): ScalaFuture[Unit] = {
    logger.info("authenticate method is invoked.")
    val login = config.login
    val password = config.password
    method(Descriptors.Authenticate, TransactionService.Authenticate.Args(login, password))
      .map(x => token = x.success.get)
  }

  def close() = channel.closeFuture().sync()
}