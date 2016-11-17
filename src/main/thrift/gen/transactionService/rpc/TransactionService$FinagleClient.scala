/**
 * Generated by Scrooge
 *   version: 4.11.0
 *   rev: 53125d3304736db41e23d8474e5765d6c2fe3ded
 *   built at: 20161011-151232
 */
package transactionService.rpc

import com.twitter.finagle.SourcedException
import com.twitter.finagle.{service => ctfs}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.{Protocols, ThriftClientRequest}
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import com.twitter.util.{Future, Return, Throw, Throwables}
import java.nio.ByteBuffer
import java.util.Arrays
import org.apache.thrift.protocol._
import org.apache.thrift.TApplicationException
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}
import scala.collection.{Map, Set}
import scala.language.higherKinds


@javax.annotation.Generated(value = Array("com.twitter.scrooge.Compiler"))
class TransactionService$FinagleClient(
    val service: com.twitter.finagle.Service[ThriftClientRequest, Array[Byte]],
    val protocolFactory: TProtocolFactory,
    val serviceName: String,
    stats: StatsReceiver,
    responseClassifier: ctfs.ResponseClassifier)
  extends TransactionService[Future] {

  def this(
    service: com.twitter.finagle.Service[ThriftClientRequest, Array[Byte]],
    protocolFactory: TProtocolFactory = Protocols.binaryFactory(),
    serviceName: String = "TransactionService",
    stats: StatsReceiver = NullStatsReceiver
  ) = this(
    service,
    protocolFactory,
    serviceName,
    stats,
    ctfs.ResponseClassifier.Default
  )

  import TransactionService._

  protected def encodeRequest(name: String, args: ThriftStruct): ThriftClientRequest = {
    val buf = new TMemoryBuffer(512)
    val oprot = protocolFactory.getProtocol(buf)

    oprot.writeMessageBegin(new TMessage(name, TMessageType.CALL, 0))
    args.write(oprot)
    oprot.writeMessageEnd()

    val bytes = Arrays.copyOfRange(buf.getArray, 0, buf.length)
    new ThriftClientRequest(bytes, false)
  }

  protected def decodeResponse[T <: ThriftStruct](
    resBytes: Array[Byte],
    codec: ThriftStructCodec[T]
  ): T = {
    val iprot = protocolFactory.getProtocol(new TMemoryInputTransport(resBytes))
    val msg = iprot.readMessageBegin()
    try {
      if (msg.`type` == TMessageType.EXCEPTION) {
        val exception = TApplicationException.read(iprot) match {
          case sourced: SourcedException =>
            if (serviceName != "") sourced.serviceName = serviceName
            sourced
          case e => e
        }
        throw exception
      } else {
        codec.decode(iprot)
      }
    } finally {
      iprot.readMessageEnd()
    }
  }

  protected def missingResult(name: String) = {
    new TApplicationException(
      TApplicationException.MISSING_RESULT,
      name + " failed: unknown result"
    )
  }

  protected def setServiceName(ex: Throwable): Throwable =
    if (this.serviceName == "") ex
    else {
      ex match {
        case se: SourcedException =>
          se.serviceName = this.serviceName
          se
        case _ => ex
      }
    }

  // ----- end boilerplate.

  private[this] val scopedStats = if (serviceName != "") stats.scope(serviceName) else stats
  private[this] object __stats_putStream {
    val RequestsCounter = scopedStats.scope("putStream").counter("requests")
    val SuccessCounter = scopedStats.scope("putStream").counter("success")
    val FailuresCounter = scopedStats.scope("putStream").counter("failures")
    val FailuresScope = scopedStats.scope("putStream").scope("failures")
  }
  
  def putStream(token: String, stream: String, partitions: Int, description: Option[String] = None): Future[Boolean] = {
    __stats_putStream.RequestsCounter.incr()
    val inputArgs = PutStream.Args(token, stream, partitions, description)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Boolean] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[PutStream.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, PutStream.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Boolean]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("putStream"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Boolean](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("putStream", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_putStream.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_putStream.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_putStream.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_isStreamExist {
    val RequestsCounter = scopedStats.scope("isStreamExist").counter("requests")
    val SuccessCounter = scopedStats.scope("isStreamExist").counter("success")
    val FailuresCounter = scopedStats.scope("isStreamExist").counter("failures")
    val FailuresScope = scopedStats.scope("isStreamExist").scope("failures")
  }
  
  def isStreamExist(token: String, stream: String): Future[Boolean] = {
    __stats_isStreamExist.RequestsCounter.incr()
    val inputArgs = IsStreamExist.Args(token, stream)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Boolean] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[IsStreamExist.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, IsStreamExist.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Boolean]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("isStreamExist"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Boolean](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("isStreamExist", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_isStreamExist.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_isStreamExist.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_isStreamExist.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_getStream {
    val RequestsCounter = scopedStats.scope("getStream").counter("requests")
    val SuccessCounter = scopedStats.scope("getStream").counter("success")
    val FailuresCounter = scopedStats.scope("getStream").counter("failures")
    val FailuresScope = scopedStats.scope("getStream").scope("failures")
  }
  
  def getStream(token: String, stream: String): Future[transactionService.rpc.Stream] = {
    __stats_getStream.RequestsCounter.incr()
    val inputArgs = GetStream.Args(token, stream)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[transactionService.rpc.Stream] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[GetStream.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, GetStream.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[transactionService.rpc.Stream]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("getStream"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[transactionService.rpc.Stream](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("getStream", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_getStream.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_getStream.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_getStream.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_delStream {
    val RequestsCounter = scopedStats.scope("delStream").counter("requests")
    val SuccessCounter = scopedStats.scope("delStream").counter("success")
    val FailuresCounter = scopedStats.scope("delStream").counter("failures")
    val FailuresScope = scopedStats.scope("delStream").scope("failures")
  }
  
  def delStream(token: String, stream: String): Future[Boolean] = {
    __stats_delStream.RequestsCounter.incr()
    val inputArgs = DelStream.Args(token, stream)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Boolean] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[DelStream.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, DelStream.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Boolean]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("delStream"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Boolean](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("delStream", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_delStream.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_delStream.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_delStream.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_putTransaction {
    val RequestsCounter = scopedStats.scope("putTransaction").counter("requests")
    val SuccessCounter = scopedStats.scope("putTransaction").counter("success")
    val FailuresCounter = scopedStats.scope("putTransaction").counter("failures")
    val FailuresScope = scopedStats.scope("putTransaction").scope("failures")
  }
  
  def putTransaction(token: String, transaction: transactionService.rpc.Transaction): Future[Boolean] = {
    __stats_putTransaction.RequestsCounter.incr()
    val inputArgs = PutTransaction.Args(token, transaction)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Boolean] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[PutTransaction.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, PutTransaction.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Boolean]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("putTransaction"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Boolean](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("putTransaction", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_putTransaction.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_putTransaction.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_putTransaction.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_putTransactions {
    val RequestsCounter = scopedStats.scope("putTransactions").counter("requests")
    val SuccessCounter = scopedStats.scope("putTransactions").counter("success")
    val FailuresCounter = scopedStats.scope("putTransactions").counter("failures")
    val FailuresScope = scopedStats.scope("putTransactions").scope("failures")
  }
  
  def putTransactions(token: String, transactions: Seq[transactionService.rpc.Transaction] = Seq[transactionService.rpc.Transaction]()): Future[Boolean] = {
    __stats_putTransactions.RequestsCounter.incr()
    val inputArgs = PutTransactions.Args(token, transactions)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Boolean] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[PutTransactions.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, PutTransactions.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Boolean]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("putTransactions"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Boolean](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("putTransactions", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_putTransactions.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_putTransactions.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_putTransactions.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_scanTransactions {
    val RequestsCounter = scopedStats.scope("scanTransactions").counter("requests")
    val SuccessCounter = scopedStats.scope("scanTransactions").counter("success")
    val FailuresCounter = scopedStats.scope("scanTransactions").counter("failures")
    val FailuresScope = scopedStats.scope("scanTransactions").scope("failures")
  }
  
  def scanTransactions(token: String, stream: String, partition: Int): Future[Seq[transactionService.rpc.Transaction]] = {
    __stats_scanTransactions.RequestsCounter.incr()
    val inputArgs = ScanTransactions.Args(token, stream, partition)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Seq[transactionService.rpc.Transaction]] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[ScanTransactions.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, ScanTransactions.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Seq[transactionService.rpc.Transaction]]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("scanTransactions"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Seq[transactionService.rpc.Transaction]](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("scanTransactions", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_scanTransactions.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_scanTransactions.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_scanTransactions.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_scanTransactionsCRC32 {
    val RequestsCounter = scopedStats.scope("scanTransactionsCRC32").counter("requests")
    val SuccessCounter = scopedStats.scope("scanTransactionsCRC32").counter("success")
    val FailuresCounter = scopedStats.scope("scanTransactionsCRC32").counter("failures")
    val FailuresScope = scopedStats.scope("scanTransactionsCRC32").scope("failures")
  }
  
  def scanTransactionsCRC32(token: String, stream: String, partition: Int): Future[Int] = {
    __stats_scanTransactionsCRC32.RequestsCounter.incr()
    val inputArgs = ScanTransactionsCRC32.Args(token, stream, partition)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Int] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[ScanTransactionsCRC32.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, ScanTransactionsCRC32.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Int]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("scanTransactionsCRC32"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Int](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("scanTransactionsCRC32", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_scanTransactionsCRC32.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_scanTransactionsCRC32.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_scanTransactionsCRC32.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_putTransactionData {
    val RequestsCounter = scopedStats.scope("putTransactionData").counter("requests")
    val SuccessCounter = scopedStats.scope("putTransactionData").counter("success")
    val FailuresCounter = scopedStats.scope("putTransactionData").counter("failures")
    val FailuresScope = scopedStats.scope("putTransactionData").scope("failures")
  }
  
  def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, data: Seq[ByteBuffer] = Seq[ByteBuffer]()): Future[Boolean] = {
    __stats_putTransactionData.RequestsCounter.incr()
    val inputArgs = PutTransactionData.Args(token, stream, partition, transaction, from, data)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Boolean] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[PutTransactionData.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, PutTransactionData.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Boolean]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("putTransactionData"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Boolean](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("putTransactionData", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_putTransactionData.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_putTransactionData.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_putTransactionData.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_getTransactionData {
    val RequestsCounter = scopedStats.scope("getTransactionData").counter("requests")
    val SuccessCounter = scopedStats.scope("getTransactionData").counter("success")
    val FailuresCounter = scopedStats.scope("getTransactionData").counter("failures")
    val FailuresScope = scopedStats.scope("getTransactionData").scope("failures")
  }
  
  def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): Future[Seq[ByteBuffer]] = {
    __stats_getTransactionData.RequestsCounter.incr()
    val inputArgs = GetTransactionData.Args(token, stream, partition, transaction, from, to)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Seq[ByteBuffer]] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[GetTransactionData.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, GetTransactionData.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Seq[ByteBuffer]]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("getTransactionData"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Seq[ByteBuffer]](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("getTransactionData", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_getTransactionData.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_getTransactionData.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_getTransactionData.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_setConsumerState {
    val RequestsCounter = scopedStats.scope("setConsumerState").counter("requests")
    val SuccessCounter = scopedStats.scope("setConsumerState").counter("success")
    val FailuresCounter = scopedStats.scope("setConsumerState").counter("failures")
    val FailuresScope = scopedStats.scope("setConsumerState").scope("failures")
  }
  
  def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): Future[Boolean] = {
    __stats_setConsumerState.RequestsCounter.incr()
    val inputArgs = SetConsumerState.Args(token, name, stream, partition, transaction)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Boolean] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[SetConsumerState.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, SetConsumerState.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Boolean]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("setConsumerState"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Boolean](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("setConsumerState", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_setConsumerState.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_setConsumerState.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_setConsumerState.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
  private[this] object __stats_getConsumerState {
    val RequestsCounter = scopedStats.scope("getConsumerState").counter("requests")
    val SuccessCounter = scopedStats.scope("getConsumerState").counter("success")
    val FailuresCounter = scopedStats.scope("getConsumerState").counter("failures")
    val FailuresScope = scopedStats.scope("getConsumerState").scope("failures")
  }
  
  def getConsumerState(token: String, name: String, stream: String, partition: Int): Future[Long] = {
    __stats_getConsumerState.RequestsCounter.incr()
    val inputArgs = GetConsumerState.Args(token, name, stream, partition)
    val replyDeserializer: Array[Byte] => _root_.com.twitter.util.Try[Long] =
      response => {
        val decodeResult: _root_.com.twitter.util.Try[GetConsumerState.Result] =
          _root_.com.twitter.util.Try {
            decodeResponse(response, GetConsumerState.Result)
          }
  
        decodeResult match {
          case t@_root_.com.twitter.util.Throw(_) =>
            t.cast[Long]
          case  _root_.com.twitter.util.Return(result) =>
            val serviceException: Throwable =
              null
  
            if (result.success.isDefined)
              _root_.com.twitter.util.Return(result.success.get)
            else if (serviceException != null)
              _root_.com.twitter.util.Throw(serviceException)
            else
              _root_.com.twitter.util.Throw(missingResult("getConsumerState"))
        }
      }
  
    val serdeCtx = new _root_.com.twitter.finagle.thrift.DeserializeCtx[Long](inputArgs, replyDeserializer)
    _root_.com.twitter.finagle.context.Contexts.local.let(
      _root_.com.twitter.finagle.thrift.DeserializeCtx.Key,
      serdeCtx
    ) {
      val serialized = encodeRequest("getConsumerState", inputArgs)
      this.service(serialized).flatMap { response =>
        Future.const(serdeCtx.deserialize(response))
      }.respond { response =>
        val responseClass = responseClassifier.applyOrElse(
          ctfs.ReqRep(inputArgs, response),
          ctfs.ResponseClassifier.Default)
        responseClass match {
          case ctfs.ResponseClass.Successful(_) =>
            __stats_getConsumerState.SuccessCounter.incr()
          case ctfs.ResponseClass.Failed(_) =>
            __stats_getConsumerState.FailuresCounter.incr()
            response match {
              case Throw(ex) =>
                setServiceName(ex)
                __stats_getConsumerState.FailuresScope.counter(Throwables.mkString(ex): _*).incr()
              case _ =>
            }
        }
      }
    }
  }
}
