package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.Descriptors._
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message, ObjectSerializer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc.{AuthInfo, ProducerTransaction, ServerException, TransactionService}
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}

class ServerHandler(transactionServer: TransactionServer, scheduledCommitLog: ScheduledCommitLog, packageTransmissionOpts: TransportOptions, implicit val context: ExecutionContext, logger: Logger) extends SimpleChannelInboundHandler[Message] {
  private val packageTooBigException = new PackageTooBigException(s"A size of client request is greater " +
    s"than maxMetadataPackageSize (${packageTransmissionOpts.maxMetadataPackageSize}) or maxDataPackageSize (${packageTransmissionOpts.maxDataPackageSize}).")

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    invokeMethod(msg, ctx.channel().remoteAddress().toString)(context).map{message =>
      val binaryMessage = message.toByteArray
      ctx.writeAndFlush(binaryMessage, ctx.voidPromise())
    }(context)
  }

  private def isTooBigMessage(message: Message) = {
    if (message.length > packageTransmissionOpts.maxMetadataPackageSize || message.length > packageTransmissionOpts.maxDataPackageSize) true else false
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    if (logger.isInfoEnabled) logger.info(s"${ctx.channel().remoteAddress().toString} is inactive")
    super.channelInactive(ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.channel().close()
   // ctx.channel().parent().close()
  }

  protected def invokeMethod(message: Message, inetAddress: String)(implicit context: ExecutionContext): ScalaFuture[Message] = {
    val isTooBigPackage = isTooBigMessage(message)
    val (method, messageSeqId) = Descriptor.decodeMethodName(message)

    def logSuccessfulProcession(): Unit = if (logger.isDebugEnabled) logger.debug(s"$inetAddress request id $messageSeqId: $method is successfully processed!")

    def logUnsuccessfulProcessing(error: Throwable): Unit = if (logger.isDebugEnabled) logger.debug(s"$inetAddress request id $messageSeqId: $method is failed while processing!", error)

    if (logger.isDebugEnabled) logger.debug(s"$inetAddress request id $messageSeqId: $method is invoked.")

    implicit val (messageId, token) = (messageSeqId, message.token)

    method match {
      case `putStreamMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.PutStream.decodeRequest(message)
            transactionServer.putStream(args.stream, args.partitions, args.description, args.ttl)
              .flatMap {
                logSuccessfulProcession()
                response => ScalaFuture.successful(Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(Some(response)))(messageId, message.token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }


      case `checkStreamExists` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.CheckStreamExists.decodeRequest(message)
            transactionServer.checkStreamExists(args.stream)
              .flatMap { response =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }


      case `getStreamMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.GetStream.decodeRequest(message)
            transactionServer.getStream(args.stream)
              .flatMap { response =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case `delStreamMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.DelStream.decodeRequest(message)
            transactionServer.delStream(args.stream)
              .flatMap { response =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case `putTransactionMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionType, message))
              .flatMap { isOkay =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(Some(isOkay)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                error.printStackTrace()
                Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case `putTransactionsMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, message))
              .flatMap { isOkay =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(Some(isOkay)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case `getTransactionMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.GetTransaction.decodeRequest(message)
            transactionServer.getTransaction(args.stream, args.partition, args.transaction)
              .flatMap { response =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case `getLastCheckpointedTransactionMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.GetLastCheckpointedTransaction.decodeRequest(message)
            transactionServer.getLastCheckpoitnedTransaction(args.stream, args.partition)
              .flatMap { response =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(response))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case `scanTransactionsMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.ScanTransactions.decodeRequest(message)
            transactionServer.scanTransactions(args.stream, args.partition, args.from, args.to, ObjectSerializer.deserialize(args.lambda).asInstanceOf[ProducerTransaction => Boolean])
              .flatMap { response =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case `putTransactionDataMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.PutTransactionData.decodeRequest(message)
            transactionServer.putTransactionData(args.stream, args.partition, args.transaction, args.data, args.from)
              .flatMap { response =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }


      case `getTransactionDataMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.GetTransactionData.decodeRequest(message)
            transactionServer.getTransactionData(args.stream, args.partition, args.transaction, args.from, args.to)
              .flatMap { response =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }


      case `putConsumerCheckpointMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(scheduledCommitLog.putData(CommitLogToBerkeleyWriter.setConsumerStateType, message))
              .flatMap { response =>
                logSuccessfulProcession()
                ScalaFuture.successful(Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case `getConsumerStateMethod` =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            val args = Descriptors.GetConsumerState.decodeRequest(message)
            transactionServer.getConsumerState(args.name, args.stream, args.partition)
              .flatMap {
                logSuccessfulProcession()
                response => ScalaFuture.successful(Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          } else {
            logUnsuccessfulProcessing(packageTooBigException)
            ScalaFuture.successful(Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token))
          }
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case `authenticateMethod` =>
        val args = Descriptors.Authenticate.decodeRequest(message)
        val response = transactionServer.authenticate(args.authKey)
        val authInfo = AuthInfo(response, packageTransmissionOpts.maxMetadataPackageSize, packageTransmissionOpts.maxDataPackageSize)
        logSuccessfulProcession()
        ScalaFuture.successful(Descriptors.Authenticate.encodeResponse(TransactionService.Authenticate.Result(Some(authInfo)))(messageId, token))

      case `isValidMethod` =>
        val args = Descriptors.IsValid.decodeRequest(message)
        val response = transactionServer.isValid(args.token)
        logSuccessfulProcession()
        ScalaFuture.successful(Descriptors.IsValid.encodeResponse(TransactionService.IsValid.Result(Some(response)))(messageId, token))
    }
  }
}