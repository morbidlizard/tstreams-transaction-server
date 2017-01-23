package com.bwsw.netty.server


import com.bwsw.netty.{Descriptors, Message}
import transactionService.rpc.TransactionService
import org.slf4j.Logger
import Descriptors._
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.apache.log4j.PropertyConfigurator

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}

class ServerHandler(transactionServer: TransactionServer, implicit val context: ExecutionContext, logger: Logger) extends SimpleChannelInboundHandler[Message] {
  PropertyConfigurator.configure("src/main/resources/logServer.properties")

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    invokeMethod(msg, ctx.channel().remoteAddress().toString)(context).map(message => ctx.writeAndFlush(message.toByteArray))(context)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    logger.info(s"${ctx.channel().remoteAddress().toString} is inactive ")
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.channel().close()
    ctx.channel().parent().close()
  }


  def invokeMethod(message: Message, inetAddress: String)(implicit context: ExecutionContext): ScalaFuture[Message] = {
    val (method, messageSeqId) = Descriptor.decodeMethodName(message)

    def logSuccessfulProcession() = logger.info(s"Server processed successfully request $method with id $messageSeqId of $inetAddress")
    def logUnSuccessfulProcession(error: Throwable) = logger.debug(error.getMessage, error)

    logger.info(s"$inetAddress request $method with id $messageSeqId")
    method match {
      case `putStreamMethod` =>
        val args = Descriptors.PutStream.decodeRequest(message)
        transactionServer.putStream(args.token, args.stream, args.partitions, args.description, args.ttl)
          .flatMap{
            logSuccessfulProcession()
            response => ScalaFuture.successful(Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }


      case `doesStreamExistMethod` =>
        val args = Descriptors.DoesStreamExist.decodeRequest(message)
        transactionServer.doesStreamExist(args.token, args.stream)
          .flatMap{response =>
            logSuccessfulProcession()
            ScalaFuture.successful(Descriptors.DoesStreamExist.encodeResponse(TransactionService.DoesStreamExist.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.DoesStreamExist.encodeResponse(TransactionService.DoesStreamExist.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }


      case `getStreamMethod` =>
        val args = Descriptors.GetStream.decodeRequest(message)
        transactionServer.getStream(args.token, args.stream)
          .flatMap{response =>
            logSuccessfulProcession()
            ScalaFuture.successful(Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }

      case `delStreamMethod` =>
        val args = Descriptors.DelStream.decodeRequest(message)
        transactionServer.delStream(args.token, args.stream)
          .flatMap{response =>
            logSuccessfulProcession()
            ScalaFuture.successful(Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }

      case `putTransactionMethod` =>
        val args = Descriptors.PutTransaction.decodeRequest(message)
        scala.concurrent.blocking(transactionServer.putTransaction(args.token, args.transaction))
          .flatMap{response =>
            logSuccessfulProcession()
            ScalaFuture.successful(Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }

      case `putTranscationsMethod` =>
        val args = Descriptors.PutTransactions.decodeRequest(message)
        scala.concurrent.blocking(transactionServer.putTransactions(args.token, args.transactions))
          .flatMap{response =>
            logSuccessfulProcession()
            ScalaFuture.successful(Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }

      case `scanTransactionsMethod` =>
        val args = Descriptors.ScanTransactions.decodeRequest(message)
        transactionServer.scanTransactions(args.token, args.stream, args.partition, args.from, args.to)
          .flatMap{response =>
            logSuccessfulProcession()
            ScalaFuture.successful(Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }

      case `putTransactionDataMethod` =>
        val args = Descriptors.PutTransactionData.decodeRequest(message)
        transactionServer.putTransactionData(args.token, args.stream, args.partition, args.transaction, args.data, args.from)
          .flatMap{response =>
            logSuccessfulProcession()
            ScalaFuture.successful(Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }


      case `getTransactionDataMethod` =>
        val args = Descriptors.GetTransactionData.decodeRequest(message)
        transactionServer.getTransactionData(args.token, args.stream, args.partition, args.transaction, args.from, args.to)
          .flatMap{response =>
            logSuccessfulProcession()
            ScalaFuture.successful(Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }


      case `setConsumerStateMethod` =>
        val args = Descriptors.SetConsumerState.decodeRequest(message)
        transactionServer.setConsumerState(args.token, args.name, args.stream, args.partition, args.transaction)
          .flatMap{response =>
            logSuccessfulProcession()
            ScalaFuture.successful(Descriptors.SetConsumerState.encodeResponse(TransactionService.SetConsumerState.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.SetConsumerState.encodeResponse(TransactionService.SetConsumerState.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }


      case `getConsumerStateMethod` =>
        val args = Descriptors.GetConsumerState.decodeRequest(message)
        transactionServer.getConsumerState(args.token, args.name, args.stream, args.partition)
          .flatMap{
            logSuccessfulProcession()
            response => ScalaFuture.successful(Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(Some(response)))(messageSeqId))
          }
          .recover { case error =>
            logUnSuccessfulProcession(error)
            Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(transactionService.rpc.ServerException(error.getMessage))))(messageSeqId)
          }

      case `authenticateMethod` =>
        val args = Descriptors.Authenticate.decodeRequest(message)
        val response = transactionServer.authenticate(args.login, args.password)
        logSuccessfulProcession()
        ScalaFuture.successful(Descriptors.Authenticate.encodeResponse(TransactionService.Authenticate.Result(Some(response)))(messageSeqId))

      case `isValidMethod` =>
        val args = Descriptors.IsValid.decodeRequest(message)
        val response = transactionServer.isValid(args.token)
        logSuccessfulProcession()
        ScalaFuture.successful(Descriptors.IsValid.encodeResponse(TransactionService.IsValid.Result(Some(response)))(messageSeqId))
    }
  }
}