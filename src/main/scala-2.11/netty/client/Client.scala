package netty.client

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import configProperties.ServerConfig.{transactionServerListen, transactionServerPort}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel

import scala.concurrent.ExecutionContext.Implicits.global
import io.netty.channel.{Channel, ChannelOption}
import netty.{Context, Descriptors}
import transactionService.rpc.{Transaction, TransactionService}

import `implicit`.Implicits._

import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise}

class Client {
  private val nextSeqId = new AtomicInteger(Int.MinValue)
  private val ReqIdToRep = new ConcurrentHashMap[Int, ScalaPromise[FunctionResult.Result]]()

  private val workerGroup = new NioEventLoopGroup()
  private val channel: Channel = {
    val b = new Bootstrap()
      .group(workerGroup)
      .channel(classOf[NioSocketChannel])
      .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .handler(new ClientInitializer(ReqIdToRep))
    val f = b.connect(transactionServerListen, transactionServerPort).sync()
    f.channel()
  }


  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Int): ScalaFuture[Boolean] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.PutStream
      .encodeRequest(TransactionService.PutStream.Args(0, stream, partitions, description, ttl))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }

  def putStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.PutStream
      .encodeRequest(TransactionService.PutStream.Args(0, stream.name, stream.partitions, stream.description, stream.ttl))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }

  def delStream(stream: String): ScalaFuture[Boolean] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.DelStream
      .encodeRequest(TransactionService.DelStream.Args(0, stream))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }

  def delStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.GetStream
      .encodeRequest(TransactionService.GetStream.Args(0, stream.name))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }

  def getStream(stream: String): ScalaFuture[transactionService.rpc.Stream] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.GetStream
      .encodeRequest(TransactionService.GetStream.Args(0, stream))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.StreamResult => fun.stream}
  }

  def doesStreamExist(stream: String): ScalaFuture[Boolean] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.DoesStreamExist
      .encodeRequest(TransactionService.DoesStreamExist.Args(0, stream))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }

  private val futurePool = Context.producerTransactionsContext.getContext

  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): ScalaFuture[Boolean] = ScalaFuture {
    val messageId = nextSeqId.getAndIncrement()

    val txns = (producerTransactions map (txn => Transaction(Some(txn), None))) ++
      (consumerTransactions map (txn => Transaction(None, Some(txn))))

    val message = Descriptors.PutTransactions
      .encodeRequest(TransactionService.PutTransactions.Args(0, txns))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }(futurePool).flatMap(identity)


  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): ScalaFuture[Boolean] = ScalaFuture {
    val messageId = nextSeqId.getAndIncrement()

    val message = Descriptors.PutTransaction
      .encodeRequest(TransactionService.PutTransaction.Args(0, Transaction(Some(transaction), None)))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }(futurePool).flatMap(identity)


  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.PutTransaction
      .encodeRequest(TransactionService.PutTransaction.Args(0, Transaction(None, Some(transaction))))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }


  def scanTransactions(stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[transactionService.rpc.ProducerTransaction]] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.ScanTransactions
      .encodeRequest(TransactionService.ScanTransactions.Args(0, stream, partition, from, to))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.SeqTransactionsResult =>
      fun.txns.withFilter(_.consumerTransaction.isEmpty).map(_.producerTransaction.get)}
  }


  def putTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.PutTransactionData
      .encodeRequest(TransactionService.PutTransactionData.Args(0, producerTransaction.stream,
        producerTransaction.partition, producerTransaction.transactionID, data, from))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }


  def putTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.PutTransactionData
      .encodeRequest(TransactionService.PutTransactionData.Args(0, consumerTransaction.stream,
        consumerTransaction.partition, consumerTransaction.transactionID, data, from))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }


  def getTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)

    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.GetTransactionData
      .encodeRequest(TransactionService.GetTransactionData.Args(0, producerTransaction.stream,
        producerTransaction.partition, producerTransaction.transactionID, from, to))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.SeqByteBufferResult => fun.bytes}
  }


  def getTransactionData(consumerTransaction: transactionService.rpc.ConsumerTransaction, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0)

    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.GetTransactionData
      .encodeRequest(TransactionService.GetTransactionData.Args(0, consumerTransaction.stream,
        consumerTransaction.partition, consumerTransaction.transactionID, from, to))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.SeqByteBufferResult => fun.bytes}
  }

  def setConsumerState(consumerTransaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.SetConsumerState
      .encodeRequest(TransactionService.SetConsumerState.Args(0, consumerTransaction.name,
        consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.BoolResult => fun.bool}
  }

  def getConsumerState(consumerTransaction: (String, String, Int)): ScalaFuture[Long] = {
    val messageId = nextSeqId.getAndIncrement()
    val message = Descriptors.GetConsumerState
      .encodeRequest(TransactionService.GetConsumerState.Args(0, consumerTransaction._1, consumerTransaction._2, consumerTransaction._3))(messageId)

    channel.writeAndFlush(message.toByteArray)

    val promise = ScalaPromise[FunctionResult.Result]
    ReqIdToRep.put(messageId, promise)
    promise.future map {case fun: FunctionResult.LongResult => fun.long}
  }

  def close() = channel.closeFuture().sync()
}

object Client extends App {
  val client = new Client()
  val rand = scala.util.Random
  val futures = (0 to 10) map { _ => ScalaFuture(client.putStream(rand.nextInt(10).toString, rand.nextInt(10000), Some(rand.nextInt(50).toString), 50)) }
  client.close()
}
