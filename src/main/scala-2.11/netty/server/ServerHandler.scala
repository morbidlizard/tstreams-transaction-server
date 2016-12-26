package netty.server

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import scala.concurrent.ExecutionContext.Implicits.global
import netty.{Descriptors, Message}
import transactionService.rpc.TransactionService

import scala.concurrent.{Future => ScalaFuture}

class ServerHandler extends SimpleChannelInboundHandler[Message] {
  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    ServerHandler.invokeMethod(msg) map (response => ctx.writeAndFlush(response.toByteArray))
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
    implicit val (method, messageSeqId) = Descriptor.decodeMethodName(message)
    method match {
      case `putStreamMethod` =>
        val args = Descriptors.PutStream.decodeRequest(message)
        transactionServer.putStream(args.token, args.stream, args.partitions, args.description, args.ttl) map {response =>
          Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(Some(response)))}

      case `doesStreamExistMethod` =>
        val args = Descriptors.DoesStreamExist.decodeRequest(message)
        transactionServer.doesStreamExist(args.token, args.stream) map (response =>
          Descriptors.DoesStreamExist.encodeResponse(TransactionService.DoesStreamExist.Result(Some(response))))

      case `getStreamMethod` =>
        val args = Descriptors.GetStream.decodeRequest(message)
        transactionServer.getStream(args.token, args.stream) map (response =>
          Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(Some(response))))

      case `delStreamMethod` =>
        val args = Descriptors.DelStream.decodeRequest(message)
        transactionServer.delStream(args.token, args.stream) map (response =>
          Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(Some(response))))

      case `putTransactionMethod` =>
        val args = Descriptors.PutTransaction.decodeRequest(message)
        transactionServer.putTransaction(args.token, args.transaction) map (response =>
          Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(Some(response))))

      case `putTranscationsMethod` =>
        val args = Descriptors.PutTransactions.decodeRequest(message)
        transactionServer.putTransactions(args.token, args.transactions) map (response =>
          Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(Some(response))))

      case `scanTransactionsMethod` =>
        val args = Descriptors.ScanTransactions.decodeRequest(message)
        transactionServer.scanTransactions(args.token, args.stream, args.partition, args.from, args.to) map (response =>
          Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(Some(response))))

      case `putTransactionDataMethod` =>
        val args = Descriptors.PutTransactionData.decodeRequest(message)
        transactionServer.putTransactionData(args.token, args.stream, args.partition, args.transaction, args.data, args.from) map (response =>
          Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(Some(response))))

      case `getTransactionDataMethod` =>
        val args = Descriptors.GetTransactionData.decodeRequest(message)
        transactionServer.getTransactionData(args.token, args.stream, args.partition, args.transaction, args.from, args.to) map (response =>
          Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(Some(response))))

      case `setConsumerStateMethod` =>
        val args = Descriptors.SetConsumerState.decodeRequest(message)
        transactionServer.setConsumerState(args.token, args.name, args.stream, args.partition, args.transaction) map (response =>
          Descriptors.SetConsumerState.encodeResponse(TransactionService.SetConsumerState.Result(Some(response))))

      case `getConsumerStateMethod` =>
        val args = Descriptors.GetConsumerState.decodeRequest(message)
        transactionServer.getConsumerState(args.token, args.name, args.stream, args.partition) map (response =>
          Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(Some(response))))

      case `authenticateMethod` =>
        val args = Descriptors.Authenticate.decodeRequest(message)
        transactionServer.authenticate(args.login, args.password) map (response =>
          Descriptors.Authenticate.encodeResponse(TransactionService.Authenticate.Result(Some(response))))

      case `isValidMethod` =>
        val args = Descriptors.IsValid.decodeRequest(message)
        transactionServer.isValid(args.token) map (response =>
          Descriptors.IsValid.encodeResponse(TransactionService.IsValid.Result(Some(response))))
    }
  }
}
