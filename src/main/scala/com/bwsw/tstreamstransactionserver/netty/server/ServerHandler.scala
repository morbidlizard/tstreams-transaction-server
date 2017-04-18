package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.Descriptors._
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message, ObjectSerializer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc._
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}

class ServerHandler(transactionServer: TransactionServer, scheduledCommitLog: ScheduledCommitLog, packageTransmissionOpts: TransportOptions, logger: Logger) extends SimpleChannelInboundHandler[Message] {
  private val packageTooBigException = new PackageTooBigException(s"A size of client request is greater " +
    s"than maxMetadataPackageSize (${packageTransmissionOpts.maxMetadataPackageSize}) or maxDataPackageSize (${packageTransmissionOpts.maxDataPackageSize}).")


  private val context: ExecutionContext = transactionServer.executionContext.context
  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    val remoteAddress = if (logger.isDebugEnabled) ctx.channel().remoteAddress().toString else ""
    invokeMethod(msg, remoteAddress)(context).map{message =>
      val binaryMessage = message.toByteArray
      ctx.writeAndFlush(binaryMessage, ctx.voidPromise())
    }(context)
  }

  private def isTooBigMetadataMessage(message: Message) = {
    message.length > packageTransmissionOpts.maxMetadataPackageSize
  }

  private def isTooBigDataMessage(message: Message) = {
    message.length > packageTransmissionOpts.maxDataPackageSize
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

  private val commitLogContext = transactionServer.executionContext.commitLogContext.getContext
  protected def invokeMethod(message: Message, inetAddress: String)(implicit context: ExecutionContext): ScalaFuture[Message] = {

    implicit val (messageId: Long, token) = (message.id, message.token)
    def isTooBigPackage = isTooBigMetadataMessage(message)

    def logSuccessfulProcession(method: String): Unit = if (logger.isDebugEnabled) logger.debug(s"$inetAddress request id ${message.id}: $method is successfully processed!")
    def logUnsuccessfulProcessing(method: String, error: Throwable): Unit = if (logger.isDebugEnabled) logger.debug(s"$inetAddress request id ${message.id}:  $method is failed while processing!", error)

    if (logger.isDebugEnabled) logger.debug(s"$inetAddress request id ${message.id} with undefined yet method is invoked.")

    message.method match {

      case Descriptors.GetCommitLogOffsets.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.GetCommitLogOffsets.encodeResponse(TransactionService.GetCommitLogOffsets.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        } else ScalaFuture {
          logSuccessfulProcession(Descriptors.GetCommitLogOffsets.name)
          Descriptors.GetCommitLogOffsets.encodeResponse(
            TransactionService.GetCommitLogOffsets.Result(
              Some(CommitLogInfo(
                transactionServer.getLastProcessedCommitLogFileID.getOrElse(-1L),
                scheduledCommitLog.currentCommitLogFile)
              )
            )
          )(messageId, message.token)
        }(context)
          .recover { case error =>
            logUnsuccessfulProcessing(Descriptors.GetCommitLogOffsets.name, error)
            Descriptors.GetCommitLogOffsets.encodeResponse(TransactionService.GetCommitLogOffsets.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
          }(context)


      case Descriptors.PutStream.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.PutStream.name, packageTooBigException)
          Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else ScalaFuture(Descriptors.PutStream.decodeRequest(message))
          .flatMap { args =>
            transactionServer.putStream(args.stream, args.partitions, args.description, args.ttl)
              .map { response =>
                logSuccessfulProcession(Descriptors.PutStream.name)
                Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(Some(response)))(messageId, message.token)
              }(context)
              .recover { case error =>
                logUnsuccessfulProcessing(Descriptors.PutStream.name, error)
                Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }
          }(context)


      case Descriptors.CheckStreamExists.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.CheckStreamExists.name, packageTooBigException)
          Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          ScalaFuture(Descriptors.CheckStreamExists.decodeRequest(message))
            .flatMap(args =>
              transactionServer.checkStreamExists(args.stream)
                .map { response =>
                  logSuccessfulProcession(Descriptors.CheckStreamExists.name)
                  Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(Some(response)))(messageId, token)
                }(context)
                .recover { case error =>
                  logUnsuccessfulProcessing(Descriptors.CheckStreamExists.name, error)
                  Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                })(context)
        }

      case Descriptors.GetStream.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.GetStream.name, packageTooBigException)
          Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          ScalaFuture(Descriptors.GetStream.decodeRequest(message))
            .flatMap(args =>
              transactionServer.getStream(args.stream)
                .map { response =>
                  logSuccessfulProcession(Descriptors.GetStream.name)
                  Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(Some(response)))(messageId, token)
                }(context)
                .recover { case error =>
                  logUnsuccessfulProcessing(Descriptors.GetStream.name, error)
                  Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                })(context)
        }


      case Descriptors.DelStream.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.DelStream.name, packageTooBigException)
          Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          ScalaFuture(Descriptors.DelStream.decodeRequest(message))
            .flatMap(args =>
              transactionServer.delStream(args.stream)
                .map { response =>
                  logSuccessfulProcession(Descriptors.DelStream.name)
                  Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(Some(response)))(messageId, token)
                }(context)
                .recover { case error =>
                  logUnsuccessfulProcessing(Descriptors.DelStream.name, error)
                  Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                })(context)
        }

      case Descriptors.PutTransaction.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.PutTransaction.name, packageTooBigException)
          Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else ScalaFuture {
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionType, message)
          logSuccessfulProcession(Descriptors.PutTransaction.name)
          Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(Some(isPutted)))(messageId, token)
        }(commitLogContext).recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutTransaction.name, error)
          Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
        }(commitLogContext)


      case Descriptors.PutTransactions.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.PutTransactions.name, packageTooBigException)
          Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else ScalaFuture {
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, message)
          logSuccessfulProcession(Descriptors.PutTransactions.name)
          Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(Some(isPutted)))(messageId, token)
        }(commitLogContext).recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutTransactions.name, error)
          Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
        }(commitLogContext)


      case Descriptors.PutSimpleTransactionAndData.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.PutSimpleTransactionAndData.name, packageTooBigException)
          Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else ScalaFuture {
          val txn = Descriptors.PutSimpleTransactionAndData.decodeRequest(message)

          transactionServer.putTransactionDataSync(txn.stream, txn.partition, txn.transaction, txn.data, txn.from)

          val transactions = Seq(
            Transaction(Some(ProducerTransaction(txn.stream, txn.partition, txn.transaction, TransactionStates.Opened, txn.data.size, 3L)), None),
            Transaction(Some(ProducerTransaction(txn.stream, txn.partition, txn.transaction, TransactionStates.Checkpointed, txn.data.size, 120L)), None)
          )

          val messageForPutTransactions = Descriptors.PutTransactions.encodeRequest(TransactionService.PutTransactions.Args(transactions))(messageId, token)

          scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, messageForPutTransactions)
          logSuccessfulProcession(Descriptors.PutSimpleTransactionAndData.name)
          Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(Some(true)))(messageId, token)
        }(commitLogContext).recover{ case error =>
          logUnsuccessfulProcessing(Descriptors.PutSimpleTransactionAndData.name, error)
          Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
        }(commitLogContext)


      case Descriptors.GetTransaction.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage)ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.GetTransaction.name, packageTooBigException)
          Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          ScalaFuture(Descriptors.GetTransaction.decodeRequest(message))
            .flatMap(args =>
              transactionServer.getTransaction(args.stream, args.partition, args.transaction)
                .flatMap { response =>
                  logSuccessfulProcession(Descriptors.GetTransaction.name)
                  ScalaFuture.successful(Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(Some(response)))(messageId, token))
                }(context)
                .recover { case error =>
                  logUnsuccessfulProcessing(Descriptors.GetTransaction.name, error)
                  Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                })(context)
        }

      case Descriptors.GetLastCheckpointedTransaction.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.GetLastCheckpointedTransaction.name, packageTooBigException)
          Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          ScalaFuture(Descriptors.GetLastCheckpointedTransaction.decodeRequest(message)).flatMap(args =>
            transactionServer.getLastCheckpointedTransaction(args.stream, args.partition)
              .map { response =>
                logSuccessfulProcession(Descriptors.GetLastCheckpointedTransaction.name)
                Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(response))(messageId, token)
              }(context)
              .recover { case error =>
                logUnsuccessfulProcessing(Descriptors.GetLastCheckpointedTransaction.name, error)
                Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              })(context)
        }

      case Descriptors.ScanTransactions.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.ScanTransactions.name, packageTooBigException)
          Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          ScalaFuture(Descriptors.ScanTransactions.decodeRequest(message)).flatMap(args =>
            transactionServer.scanTransactions(args.stream, args.partition, args.from, args.to, ObjectSerializer.deserialize(args.lambda).asInstanceOf[ProducerTransaction => Boolean])
              .map { response =>
                logSuccessfulProcession(Descriptors.ScanTransactions.name)
                Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(Some(response)))(messageId, token)
              }(context)
              .recover { case error =>
                logUnsuccessfulProcessing(Descriptors.ScanTransactions.name, error)
                Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              })(context)
        }

      case Descriptors.PutTransactionData.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigDataMessage(message)) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.PutTransactionData.name, packageTooBigException)
          Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          ScalaFuture(Descriptors.PutTransactionData.decodeRequest(message))
            .flatMap(args =>
              transactionServer.putTransactionData(args.stream, args.partition, args.transaction, args.data, args.from)
                .map { response =>
                  logSuccessfulProcession(Descriptors.PutTransactionData.name)
                  Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(Some(response)))(messageId, token)
                }(context)
                .recover { case error =>
                  logUnsuccessfulProcessing(Descriptors.PutTransactionData.name, error)
                  Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                })(context)
        }

      case Descriptors.GetTransactionData.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.GetTransactionData.name, packageTooBigException)
          Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          ScalaFuture(Descriptors.GetTransactionData.decodeRequest(message))
            .flatMap(args =>
              transactionServer.getTransactionData(args.stream, args.partition, args.transaction, args.from, args.to)
                .flatMap { response =>
                  logSuccessfulProcession(Descriptors.GetTransactionData.name)
                  ScalaFuture.successful(Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(Some(response)))(messageId, token))
                }(context)
                .recover { case error =>
                  logUnsuccessfulProcessing(Descriptors.GetTransactionData.name, error)
                  Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                })(context)
        }


      case Descriptors.PutConsumerCheckpoint.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.PutConsumerCheckpoint.name, packageTooBigException)
          Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.setConsumerStateType, message)
          ScalaFuture {
            logSuccessfulProcession(Descriptors.PutConsumerCheckpoint.name)
            Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(Some(isPutted)))(messageId, token)
          }(commitLogContext)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.PutConsumerCheckpoint.name, error)
              Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
            }(commitLogContext)
        }

      case Descriptors.GetConsumerState.methodID =>
        if (!transactionServer.isValid(message.token)) {
          ScalaFuture.successful(Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }
        else if (isTooBigPackage) ScalaFuture {
          logUnsuccessfulProcessing(Descriptors.GetConsumerState.name, packageTooBigException)
          Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
        }(context)
        else {
          ScalaFuture {
            Descriptors.GetConsumerState.decodeRequest(message)
          } flatMap (args =>
            transactionServer.getConsumerState(args.name, args.stream, args.partition)
              .flatMap {
                logSuccessfulProcession(Descriptors.GetConsumerState.name)
                response => ScalaFuture.successful(Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(Some(response)))(messageId, token))
              }
              .recover { case error =>
                logUnsuccessfulProcessing(Descriptors.GetConsumerState.name, error)
                Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              })
        }

      case Descriptors.Authenticate.methodID => ScalaFuture {
        val args = Descriptors.Authenticate.decodeRequest(message)
        val response = transactionServer.authenticate(args.authKey)
        val authInfo = AuthInfo(response, packageTransmissionOpts.maxMetadataPackageSize, packageTransmissionOpts.maxDataPackageSize)
        logSuccessfulProcession(Descriptors.Authenticate.name)
        Descriptors.Authenticate.encodeResponse(TransactionService.Authenticate.Result(Some(authInfo)))(messageId, token)
      }


      case Descriptors.IsValid.methodID => ScalaFuture {
        val args = Descriptors.IsValid.decodeRequest(message)
        val response = transactionServer.isValid(args.token)
        logSuccessfulProcession(Descriptors.IsValid.name)
        Descriptors.IsValid.encodeResponse(TransactionService.IsValid.Result(Some(response)))(messageId, token)
      }
    }
  }
}