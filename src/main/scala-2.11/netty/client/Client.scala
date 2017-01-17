package netty.client

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import `implicit`.Implicits._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.twitter.scrooge.ThriftStruct
import configProperties.ClientConfig._
import exception.Throwables.ZkGetMasterException
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder
import netty.{Context, Descriptors, MessageDecoder}
import org.apache.curator.retry.RetryNTimes
import transactionService.rpc.{TransactionService, _}
import zooKeeper.ZKLeaderClient

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}

class Client {
  val zKLeaderClient = new ZKLeaderClient(zkEndpoints, zkTimeoutSession, zkTimeoutConnection,
    new RetryNTimes(zkRetriesMax, zkTimeoutBetweenRetries), zkPrefix)
  zKLeaderClient.start()

  private implicit final val context = Context(Executors.newFixedThreadPool(
    configProperties.ClientConfig.clientPool,
    new ThreadFactoryBuilder().setNameFormat("ClientPool-%d").build())
  ).getContext

  private val nextSeqId = new AtomicInteger(Int.MinValue)
  private val ReqIdToRep = new ConcurrentHashMap[Int, ScalaPromise[ThriftStruct]](10000, 1.0f, configProperties.ClientConfig.clientPool)
  private val workerGroup = new EpollEventLoopGroup()
  createBootstrap(new Bootstrap(), workerGroup)
  var channel: Channel = _
  //  lazy val bootstrap = new Bootstrap()
  //    .group(workerGroup)
  //    .channel(classOf[EpollSocketChannel])
  //    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)
  //    .handler(new ClientInitializer(ReqIdToRep, this ,context))


  def createBootstrap(bootstrap: Bootstrap, eventLoop: EventLoopGroup) = {
    if (bootstrap != null) {
      val handler = new ClientHandler(ReqIdToRep, this, context)
      bootstrap.group(eventLoop)
      bootstrap.channel(classOf[EpollSocketChannel])
      bootstrap.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      bootstrap.handler(new ChannelInitializer[SocketChannel]() {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline()
            .addLast(new ByteArrayEncoder())
            .addLast(new MessageDecoder)
            .addLast(handler)
        }
      })
      val (listen, port) = getInetAddressFromZookeeper(zkTimeoutConnection / zkTimeoutBetweenRetries)
      bootstrap.remoteAddress(listen, port)
      bootstrap.connect().addListener(new ConnectionListener(this))
    }

    bootstrap
  }

  @tailrec
  final def getInetAddressFromZookeeper(times: Int): (String, Int) = {
    if (times > 0 && zKLeaderClient.master.isEmpty) {
      TimeUnit.MILLISECONDS.sleep(zkTimeoutBetweenRetries)
      getInetAddressFromZookeeper(times - 1)
    } else {
      zKLeaderClient.master match {
        case Some(master) => val listenPort = master.split(":")
          (listenPort(0), listenPort(1).toInt)
        case None => throw new ZkGetMasterException
      }
    }
  }

  //  def connect(): ChannelFuture = {
  //    val (listen, port) = getInetAddressFromZookeeper(zkTimeoutConnection / zkTimeoutBetweenRetries)
  //    val channelFuture = bootstrap.connect(listen, port)
  //    val listener = new ChannelFutureListener() {
  //      val atomicInteger = new AtomicInteger(serverTimeoutConnection / serverTimeoutBetweenRetries)
  //
  //      @throws[Exception]
  //      override def operationComplete(future: ChannelFuture): Unit = {
  //        if (!future.isSuccess && atomicInteger.getAndDecrement() > 0) {
  //          future.channel().close()
  //          TimeUnit.MILLISECONDS.sleep(serverTimeoutBetweenRetries)
  //          future.addListener(this)
  //        } else if (atomicInteger.get() <= 0) throw new ServerConnectionException
  //      }
  //    }
  //    channelFuture.addListener(listener)
  //  }

  private def method[Req <: ThriftStruct, Rep <: ThriftStruct](descriptor: Descriptors.Descriptor[Req, Rep], request: Req)(implicit context: ExecutionContext): ScalaFuture[Rep] = {
    if (channel.isActive) {
      val messageId = nextSeqId.getAndIncrement()
      val promise = ScalaPromise[ThriftStruct]
      val message = descriptor.encodeRequest(request)(messageId)
      ReqIdToRep.put(messageId, promise)
      channel.writeAndFlush(message.toByteArray)
      promise.future.map { response =>
        ReqIdToRep.remove(messageId)
        response.asInstanceOf[Rep]
      }
    } else ScalaFuture.failed(new exception.Throwables.ServerUnreachableException)
  }



  private def retry[Req, Rep](times: Int)(f: => ScalaFuture[Rep])(condition: PartialFunction[Throwable, Boolean]): ScalaFuture[Rep] = {
    def helper(times: Int)(f: => ScalaFuture[Rep]): ScalaFuture[Rep] = f.recoverWith {
      case error if times > 0 && condition(error) =>
        helper(times - 1)(f)
    }

    helper(times)(f)
  }

  private def retryAuthenticate[Req, Rep](f: => ScalaFuture[Rep]) = retry(50)(f)(
    new PartialFunction[Throwable, Boolean] {
      override def apply(v1: Throwable): Boolean = v1 match {
        case _: exception.Throwables.TokenInvalidException =>
          authenticate()
          TimeUnit.MILLISECONDS.sleep(authTokenTimeoutBetweenRetries)
          true
        case _: exception.Throwables.ServerUnreachableException =>
          TimeUnit.MILLISECONDS.sleep(authTokenTimeoutBetweenRetries)
          true
        case error =>
          println(error.getMessage)
          //          System.err.println(error.getMessage)
          false
      }

      override def isDefinedAt(x: Throwable): Boolean = x.isInstanceOf[exception.Throwables.TokenInvalidException]
    }
  )

  @volatile private var token: Int = _
  //Await.ready(authenticate(), 5 seconds)

  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Int): ScalaFuture[Boolean] = {
    retryAuthenticate(method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream, partitions, description, ttl))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  def putStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    retryAuthenticate(method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream.name, stream.partitions, stream.description, stream.ttl))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  def delStream(stream: String): ScalaFuture[Boolean] = {
    retryAuthenticate(method(Descriptors.DelStream, TransactionService.DelStream.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get)))
  }

  def delStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    retryAuthenticate(method(Descriptors.DelStream, TransactionService.DelStream.Args(token, stream.name))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  def getStream(stream: String): ScalaFuture[transactionService.rpc.Stream] = {
    retryAuthenticate(method(Descriptors.GetStream, TransactionService.GetStream.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  def doesStreamExist(stream: String): ScalaFuture[Boolean] = {
    retryAuthenticate(method(Descriptors.DoesStreamExist, TransactionService.DoesStreamExist.Args(token, stream))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  private final val futurePool = Context(1, "ClientTransactionPool-%d").getContext

  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): ScalaFuture[Boolean] = {

    val txns = (producerTransactions map (txn => Transaction(Some(txn), None))) ++
      (consumerTransactions map (txn => Transaction(None, Some(txn))))

    retryAuthenticate(method(Descriptors.PutTransactions, TransactionService.PutTransactions.Args(token, txns))(futurePool)
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool)
    )
  }


  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): ScalaFuture[Boolean] = {
    TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None))
    retryAuthenticate(method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None)))(futurePool)
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool))
  }


  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    retryAuthenticate(method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(None, Some(transaction))))(futurePool)
      .flatMap(x => /*if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else*/ ScalaFuture.successful(true /*x.success.get*/))(futurePool))
  }


  def scanTransactions(stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[transactionService.rpc.ProducerTransaction]] = {
    retryAuthenticate(method(Descriptors.ScanTransactions, TransactionService.ScanTransactions.Args(token, stream, partition, from, to))
      .flatMap(x =>
        if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message))
        else
          ScalaFuture.successful(x.success.get.withFilter(_.consumerTransaction.isEmpty).map(_.producerTransaction.get))
      )
    )
  }


  def putTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    retryAuthenticate(method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, producerTransaction.stream,
      producerTransaction.partition, producerTransaction.transactionID, data, from))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  def putTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    retryAuthenticate(method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, consumerTransaction.stream,
      consumerTransaction.partition, consumerTransaction.transactionID, data, from))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  def getTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)

    retryAuthenticate(method(Descriptors.GetTransactionData, TransactionService.GetTransactionData.Args(token, producerTransaction.stream,
      producerTransaction.partition, producerTransaction.transactionID, from, to))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  def getTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)

    retryAuthenticate(method(Descriptors.GetTransactionData, TransactionService.GetTransactionData.Args(token, consumerTransaction.stream,
      consumerTransaction.partition, consumerTransaction.transactionID, from, to))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  def setConsumerState(consumerTransaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    retryAuthenticate(method(Descriptors.SetConsumerState, TransactionService.SetConsumerState.Args(token, consumerTransaction.name,
      consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool)
    )
  }

  def getConsumerState(consumerTransaction: (String, String, Int)): ScalaFuture[Long] = {
    retryAuthenticate(method(Descriptors.GetConsumerState, TransactionService.GetConsumerState.Args(token, consumerTransaction._1, consumerTransaction._2, consumerTransaction._3))
      .flatMap(x => if (x.error.isDefined) ScalaFuture.failed(exception.Throwables.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  private def authenticate(): ScalaFuture[Unit] = {
    val login = configProperties.ClientConfig.login
    val password = configProperties.ClientConfig.password
    method(Descriptors.Authenticate, TransactionService.Authenticate.Args(login, password))
      .map(x => token = x.success.get)
  }

  //def close() = channelGroup.newCloseFuture().sync()
}

object Client {
  private def retry(times: Int)(f: => Int): Int = {
    def helper(times: Int)(f: => Int): Int = f

    helper(times)(f)
  }

  def a = {
    println(1)
    1
  }

  def main(args: Array[String]): Unit = {
    retry(2)(a)
  }
}