package netty.client

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import configProperties.ServerConfig.{transactionServerListen, transactionServerPort}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{Channel, ChannelOption, WriteBufferWaterMark}
import netty.{Context, Descriptors}
import transactionService.rpc.{TransactionService, _}
import `implicit`.Implicits._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.twitter.scrooge.ThriftStruct
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}

import scala.concurrent.{Await, ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}

class Client {
  private implicit val context = Context(Executors.newFixedThreadPool(
    configProperties.ClientConfig.clientPool,
    new ThreadFactoryBuilder().setNameFormat("ClientPool-%d").build())
  ).getContext

  private val nextSeqId = new AtomicInteger(Int.MinValue)
  private val ReqIdToRep = new ConcurrentHashMap[Int, ScalaPromise[ThriftStruct]](10000, 0.5f, configProperties.ClientConfig.clientPool)

  private val workerGroup = new EpollEventLoopGroup()
  private val channel: Channel = {
    val b = new Bootstrap()
      .group(workerGroup)
      .channel(classOf[EpollSocketChannel])
      .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .handler(new ClientInitializer(ReqIdToRep,context))
    val f = b.connect(transactionServerListen, transactionServerPort).sync()
    f.channel()
  }

  private def method[Req <: ThriftStruct, Rep <: ThriftStruct](descriptor: Descriptors.Descriptor[Req, Rep], request: Req)(implicit context: ExecutionContext): ScalaFuture[Rep] = {
    val messageId = nextSeqId.getAndIncrement()
    val promise = ScalaPromise[ThriftStruct]
    ReqIdToRep.put(messageId, promise)
    val message = descriptor.encodeRequest(request)(messageId)
    channel.writeAndFlush(message.toByteArray, channel.voidPromise())
    promise.future.map { response =>
      ReqIdToRep.remove(messageId)
      response.asInstanceOf[Rep]
    }
  }

    private def retry[Req, Rep](times: Int, timeUnit: TimeUnit, amount: Long)(f: => ScalaFuture[Rep])(implicit context: ExecutionContext): ScalaFuture[Rep] = {
      def helper(times: Int)(f: => ScalaFuture[Rep]): ScalaFuture[Rep] = f recoverWith {
        case _ if times > 0 =>
          timeUnit.sleep(amount)
          authenticate() flatMap (_ => helper(times - 1)(f))
      }
      helper(times)(f)
    }


  @volatile private var token: Int = _
  import scala.concurrent.duration._
  //Await.ready(authenticate(), 5 seconds)

  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Int): ScalaFuture[Boolean] = {
    retry(5, TimeUnit.SECONDS, 100)(method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream, partitions, description, ttl))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get)))
  }

  def putStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    method(Descriptors.PutStream, TransactionService.PutStream.Args(token, stream.name, stream.partitions, stream.description, stream.ttl))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
  }

  def delStream(stream: String): ScalaFuture[Boolean] = {
    method(Descriptors.DelStream, TransactionService.DelStream.Args(token, stream))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
  }

  def delStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    method(Descriptors.DelStream,TransactionService.DelStream.Args(token, stream.name))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
  }

  def getStream(stream: String): ScalaFuture[transactionService.rpc.Stream] = {
    method(Descriptors.GetStream, TransactionService.GetStream.Args(token, stream))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
  }

  def doesStreamExist(stream: String): ScalaFuture[Boolean] = {
    method(Descriptors.DoesStreamExist, TransactionService.DoesStreamExist.Args(token, stream))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
  }

  private val futurePool = Context(1, "ClientTransactionPool-%d").getContext
  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): ScalaFuture[Boolean] = {

    val txns = (producerTransactions map (txn => Transaction(Some(txn), None))) ++
      (consumerTransactions map (txn => Transaction(None, Some(txn))))

    method(Descriptors.PutTransactions, TransactionService.PutTransactions.Args(token, txns))(futurePool)
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))(futurePool)
  }


  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): ScalaFuture[Boolean] = {
    TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None))
    method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None)))(futurePool)
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))(futurePool)
      .recover{case error => println(error); true}
  }


  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    method(Descriptors.PutTransaction, TransactionService.PutTransaction.Args(token, Transaction(None, Some(transaction))))(futurePool)
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))(futurePool)
      .recover{case error => println(error); true}
  }


  def scanTransactions(stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[transactionService.rpc.ProducerTransaction]] = {
    method(Descriptors.ScanTransactions, TransactionService.ScanTransactions.Args(token, stream, partition, from, to))
      .map(x => x.success.get.withFilter(_.consumerTransaction.isEmpty).map(_.producerTransaction.get))
  }


  def putTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, producerTransaction.stream,
      producerTransaction.partition, producerTransaction.transactionID, data, from))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
      .recover{case error => println(error); true}
  }


  def putTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    method(Descriptors.PutTransactionData, TransactionService.PutTransactionData.Args(token, consumerTransaction.stream,
      consumerTransaction.partition, consumerTransaction.transactionID, data, from))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
  }


  def getTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)

    method(Descriptors.GetTransactionData, TransactionService.GetTransactionData.Args(token, producerTransaction.stream,
      producerTransaction.partition, producerTransaction.transactionID, from, to))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
  }


  def getTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)

    method(Descriptors.GetTransactionData, TransactionService.GetTransactionData.Args(token, consumerTransaction.stream,
      consumerTransaction.partition, consumerTransaction.transactionID, from, to))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
  }

  def setConsumerState(consumerTransaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    method(Descriptors.SetConsumerState,  TransactionService.SetConsumerState.Args(token, consumerTransaction.name,
      consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))(futurePool)
  }

  def getConsumerState(consumerTransaction: (String, String, Int)): ScalaFuture[Long] = {
    method(Descriptors.GetConsumerState,  TransactionService.GetConsumerState.Args(token, consumerTransaction._1, consumerTransaction._2, consumerTransaction._3))
      .flatMap(x => if (x.tokenInvalid.isDefined) ScalaFuture.failed(new Exception(x.tokenInvalid.get)) else ScalaFuture.successful(x.success.get))
  }

  private def authenticate(): ScalaFuture[Unit] = {
    val login = configProperties.ClientConfig.login
    val password = configProperties.ClientConfig.password
    method(Descriptors.Authenticate, TransactionService.Authenticate.Args(login, password))
      .map(x => token = x.success.get)
  }

  def close() = channel.closeFuture().sync()
}

object Client extends App {
//  private implicit lazy val context = netty.Context.clientPool.getContext
//
//  val rand = scala.util.Random
//  private def getRandomStream = new transactionService.rpc.Stream {
//    override val name: String = rand.nextInt(10000).toString
//    override val partitions: Int = rand.nextInt(10000)
//    override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
//    override val ttl: Int = rand.nextInt(Int.MaxValue)
//  }
//  private def chooseStreamRandomly(streams: IndexedSeq[transactionService.rpc.Stream]) = streams(rand.nextInt(streams.length))
//
//  private def getRandomProducerTransaction(streamObj: transactionService.rpc.Stream) = new ProducerTransaction {
//    override val transactionID: Long = System.nanoTime()
//    override val state: TransactionStates = TransactionStates(rand.nextInt(TransactionStates(2).value) + 1)
//    override val stream: String = streamObj.name
//    override val keepAliveTTL: Long = Long.MaxValue
//    override val quantity: Int = -1
//    override val partition: Int = streamObj.partitions
//  }
//
//  private def getRandomConsumerTransaction(streamObj: transactionService.rpc.Stream) =  new ConsumerTransaction {
//    override def transactionID: Long = scala.util.Random.nextLong()
//    override def name: String = rand.nextInt(10000).toString
//    override def stream: String = streamObj.name
//    override def partition: Int = streamObj.partitions
//  }
//  val client = new Client()
//
//  client.authenticate() map { _ =>
//    import scala.concurrent.duration._
//    val stream = getRandomStream
//    println(Await.result(client.putStream(stream), 10.seconds))
//
//    val txn = getRandomProducerTransaction(stream)
//    Await.result(client.putTransaction(txn), 10.seconds)
//
//    val amount = 50000
//    val data = Array.fill(amount)(rand.nextString(10).getBytes)
//
//    val before = System.currentTimeMillis()
//    Await.result(client.putTransactionData(txn, data, 0), 10.seconds)
//    val after = System.currentTimeMillis()
//    println(after - before)
//
//    val res = Await.result(client.getTransactionData(txn, 0, 50000), 10.seconds)
//
//    res
//  } map (_ =>  client.close())
}
