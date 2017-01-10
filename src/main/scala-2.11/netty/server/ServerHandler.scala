package netty.server

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import netty.{Descriptors, Message}
import transactionService.rpc.{TokenInvalidException, TransactionService}

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}

class ServerHandler extends SimpleChannelInboundHandler[Message] {
  private implicit val context = netty.Context.serverPool.getContext

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    scala.concurrent.blocking(ServerHandler.invokeMethod(msg))
      .map(message => ctx.writeAndFlush(message.toByteArray))
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    println("User is inactive")
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}

private object ServerHandler {
  val transactionServer = new TransactionServer()
  import Descriptors._

  def invokeMethod(message: Message)(implicit context: ExecutionContext): ScalaFuture[Message] = {
    val (method, messageSeqId) = Descriptor.decodeMethodName(message)
    method match {
      case `putStreamMethod` =>
        val args = Descriptors.PutStream.decodeRequest(message)
        transactionServer.putStream(args.token, args.stream, args.partitions, args.description, args.ttl)
          .flatMap(response => ScalaFuture.successful(Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(Some(response)))(messageSeqId)))
          .recover { case error =>
            println(error.getMessage)
            Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }


      case `doesStreamExistMethod` =>
        val args = Descriptors.DoesStreamExist.decodeRequest(message)
        transactionServer.doesStreamExist(args.token, args.stream)
          .flatMap(response => ScalaFuture.successful(Descriptors.DoesStreamExist.encodeResponse(TransactionService.DoesStreamExist.Result(Some(response)))(messageSeqId)))
          .recover { case _ =>
            Descriptors.DoesStreamExist.encodeResponse(TransactionService.DoesStreamExist.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }


      case `getStreamMethod` =>
        val args = Descriptors.GetStream.decodeRequest(message)
        transactionServer.getStream(args.token, args.stream)
          .flatMap(response => ScalaFuture.successful(Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(Some(response)))(messageSeqId)))
          .recover { case _ =>
            Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }

      case `delStreamMethod` =>
        val args = Descriptors.DelStream.decodeRequest(message)
        transactionServer.delStream(args.token, args.stream)
          .flatMap(response => ScalaFuture.successful(Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(Some(response)))(messageSeqId)))
          .recover { case _ =>
            Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }

      case `putTransactionMethod` =>
        val args = Descriptors.PutTransaction.decodeRequest(message)
        transactionServer.putTransaction(args.token, args.transaction)
          .flatMap(response => ScalaFuture.successful(Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(Some(response)))(messageSeqId)))
          .recover { case error =>
            println(error.getMessage)
            Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }

      case `putTranscationsMethod` =>
        val args = Descriptors.PutTransactions.decodeRequest(message)
        transactionServer.putTransactions(args.token, args.transactions)
          .flatMap(response => ScalaFuture.successful(Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(Some(response)))(messageSeqId)))
          .recover { case error =>
            println(error.getMessage)
            Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }

      case `scanTransactionsMethod` =>
        val args = Descriptors.ScanTransactions.decodeRequest(message)
        transactionServer.scanTransactions(args.token, args.stream, args.partition, args.from, args.to)
          .flatMap(response => ScalaFuture.successful(Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(Some(response)))(messageSeqId)))
          .recover { case _ =>
            Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }

      case `putTransactionDataMethod` =>
        val args = Descriptors.PutTransactionData.decodeRequest(message)
        transactionServer.putTransactionData(args.token, args.stream, args.partition, args.transaction, args.data, args.from)
          .flatMap(response => ScalaFuture.successful(Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(Some(response)))(messageSeqId)))
          .recover { case _ =>
            Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }


      case `getTransactionDataMethod` =>
        val args = Descriptors.GetTransactionData.decodeRequest(message)
        transactionServer.getTransactionData(args.token, args.stream, args.partition, args.transaction, args.from, args.to)
          .flatMap(response => ScalaFuture.successful(Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(Some(response)))(messageSeqId)))
          .recover { case _ =>
            Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }


      case `setConsumerStateMethod` =>
        val args = Descriptors.SetConsumerState.decodeRequest(message)
        transactionServer.setConsumerState(args.token, args.name, args.stream, args.partition, args.transaction)
          .flatMap(response => ScalaFuture.successful(Descriptors.SetConsumerState.encodeResponse(TransactionService.SetConsumerState.Result(Some(response)))(messageSeqId)))
          .recover { case _ =>
            Descriptors.SetConsumerState.encodeResponse(TransactionService.SetConsumerState.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }


      case `getConsumerStateMethod` =>
        val args = Descriptors.GetConsumerState.decodeRequest(message)
        transactionServer.getConsumerState(args.token, args.name, args.stream, args.partition)
          .flatMap(response => ScalaFuture.successful(Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(Some(response)))(messageSeqId)))
          .recover { case _ =>
            Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
          }

      case `authenticateMethod` =>
        val args = Descriptors.Authenticate.decodeRequest(message)
        val response = transactionServer.authenticate(args.login, args.password)
        ScalaFuture.successful(Descriptors.Authenticate.encodeResponse(TransactionService.Authenticate.Result(Some(response)))(messageSeqId))

      case `isValidMethod` =>
        val args = Descriptors.IsValid.decodeRequest(message)
        val response = transactionServer.isValid(args.token)
        ScalaFuture.successful(Descriptors.IsValid.encodeResponse(TransactionService.IsValid.Result(Some(response)))(messageSeqId))
    }
  }
}
