package netty.server

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import scala.concurrent.ExecutionContext.Implicits.global
import netty.{Descriptors, Message}
import transactionService.rpc.{TokenInvalidException, TransactionService}

import scala.concurrent.{Future => ScalaFuture}

class ServerHandler extends SimpleChannelInboundHandler[Message] {
  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    ServerHandler.invokeMethod(msg).map(message => ctx.writeAndFlush(message.toByteArray))
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
  def invokeMethod(message: Message): ScalaFuture[Message] = {
    val (method, messageSeqId) = Descriptor.decodeMethodName(message)
    method match {
      case `putStreamMethod` =>
        val args = Descriptors.PutStream.decodeRequest(message)
        transactionServer.putStream(args.token, args.stream, args.partitions, args.description, args.ttl)
          .map(response => Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(Some(response)))(messageSeqId))
          .recover{case error =>
            println(error.getMessage)
            Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `doesStreamExistMethod` =>
        val args = Descriptors.DoesStreamExist.decodeRequest(message)
        transactionServer.doesStreamExist(args.token, args.stream)
          .map (response => Descriptors.DoesStreamExist.encodeResponse(TransactionService.DoesStreamExist.Result(Some(response)))(messageSeqId))
          .recover{case _ =>
            Descriptors.DoesStreamExist.encodeResponse(TransactionService.DoesStreamExist.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `getStreamMethod` =>
        val args = Descriptors.GetStream.decodeRequest(message)
        transactionServer.getStream(args.token, args.stream)
          .map (response => Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(Some(response)))(messageSeqId))
          .recover{case _ =>
            Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `delStreamMethod` =>
        val args = Descriptors.DelStream.decodeRequest(message)
        transactionServer.delStream(args.token, args.stream)
          .map (response => Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(Some(response)))(messageSeqId))
          .recover{case _ =>
            Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `putTransactionMethod` =>
        val args = Descriptors.PutTransaction.decodeRequest(message)
        transactionServer.putTransaction(args.token, args.transaction)
          .map (response => Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(Some(response)))(messageSeqId))
          .recover{case error =>
            println(error.getMessage)
            Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `putTranscationsMethod` =>
        val args = Descriptors.PutTransactions.decodeRequest(message)
        transactionServer.putTransactions(args.token, args.transactions)
          .map (response => Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(Some(response)))(messageSeqId))
          .recover{case error =>
            println(error.getMessage)
            Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `scanTransactionsMethod` =>
        val args = Descriptors.ScanTransactions.decodeRequest(message)
        transactionServer.scanTransactions(args.token, args.stream, args.partition, args.from, args.to)
          .map (response => Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(Some(response)))(messageSeqId))
          .recover{case _ =>
            Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `putTransactionDataMethod` =>
        val args = Descriptors.PutTransactionData.decodeRequest(message)
        transactionServer.putTransactionData(args.token, args.stream, args.partition, args.transaction, args.data, args.from)
          .map (response => Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(Some(response)))(messageSeqId))
          .recover{case _ =>
            Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `getTransactionDataMethod` =>
        val args = Descriptors.GetTransactionData.decodeRequest(message)
        transactionServer.getTransactionData(args.token, args.stream, args.partition, args.transaction, args.from, args.to)
          .map (response => Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(Some(response)))(messageSeqId))
          .recover{case _ =>
            Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `setConsumerStateMethod` =>
        val args = Descriptors.SetConsumerState.decodeRequest(message)
        transactionServer.setConsumerState(args.token, args.name, args.stream, args.partition, args.transaction)
          .map (response => Descriptors.SetConsumerState.encodeResponse(TransactionService.SetConsumerState.Result(Some(response)))(messageSeqId))
          .recover{case _ =>
            Descriptors.SetConsumerState.encodeResponse(TransactionService.SetConsumerState.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `getConsumerStateMethod` =>
        val args = Descriptors.GetConsumerState.decodeRequest(message)
        transactionServer.getConsumerState(args.token, args.name, args.stream, args.partition)
          .map (response => Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(Some(response)))(messageSeqId))
          .recover{case _ =>
            Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)}

      case `authenticateMethod` =>
        val args = Descriptors.Authenticate.decodeRequest(message)
        transactionServer.authenticate(args.login, args.password) map {response =>
          Descriptors.Authenticate.encodeResponse(TransactionService.Authenticate.Result(Some(response)))(messageSeqId)}

      case `isValidMethod` =>
        val args = Descriptors.IsValid.decodeRequest(message)
        transactionServer.isValid(args.token) map (response =>
          Descriptors.IsValid.encodeResponse(TransactionService.IsValid.Result(Some(response)))(messageSeqId))
    }
  }
}
