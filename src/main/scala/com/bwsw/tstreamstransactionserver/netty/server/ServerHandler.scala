package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc._
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}
import scala.util.Try

class ServerHandler(transactionServer: TransactionServer, scheduledCommitLog: ScheduledCommitLog, packageTransmissionOpts: TransportOptions, logger: Logger) extends SimpleChannelInboundHandler[ByteBuf] {
  private lazy val packageTooBigException = new PackageTooBigException(s"A size of client request is greater " +
    s"than maxMetadataPackageSize (${packageTransmissionOpts.maxMetadataPackageSize}) or maxDataPackageSize (${packageTransmissionOpts.maxDataPackageSize}).")

  private val serverWriteContext: ExecutionContext = transactionServer.executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext = transactionServer.executionContext.serverReadContext

  override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
    val message = Message.fromByteBuf(buf)
    invokeMethod(message, ctx)
  }

  private def isTooBigMetadataMessage(message: Message) = {
    message.length > packageTransmissionOpts.maxMetadataPackageSize
  }

  private def isTooBigDataMessage(message: Message) = {
    message.length > packageTransmissionOpts.maxDataPackageSize
  }

  @volatile var isChannelActive = true

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    isChannelActive = false
    //    if (logger.isInfoEnabled) logger.info(s"${ctx.channel().remoteAddress().toString} is inactive")
    ctx.fireChannelInactive()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.channel().close()
    // ctx.channel().parent().close()
  }

  @inline
  private def sendResponseToClient(message: => Message, ctx: ChannelHandlerContext, isFireAndForgetMethod: Boolean): Unit = {
    if (!isFireAndForgetMethod) {
      val binaryResponse = message.toByteArray
      if (isChannelActive) ctx.writeAndFlush(binaryResponse)
    }
  }

  private val commitLogContext = transactionServer.executionContext.commitLogContext

  protected def invokeMethod(message: Message, ctx: ChannelHandlerContext): Unit = {

    implicit val (messageId: Long, token) = (message.id, message.token)

    def logSuccessfulProcession(method: String): Unit = if (logger.isDebugEnabled) logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}: $method is successfully processed!")

    def logUnsuccessfulProcessing(method: String, error: Throwable): Unit = if (logger.isDebugEnabled) logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}:  $method is failed while processing!", error)

    if (logger.isDebugEnabled) logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id} method is invoked.")

    val isFireAndForgetMethod = if (message.isFireAndForgetMethod == (1:Byte)) true else false
    message.method match {

      case Descriptors.GetCommitLogOffsets.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.GetCommitLogOffsets.encodeResponseToMessage(TransactionService.GetCommitLogOffsets.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        } else {
          val result: Try[Message] = scala.util.Try(Descriptors.GetCommitLogOffsets.encodeResponseToMessage(
            TransactionService.GetCommitLogOffsets.Result(
              Some(CommitLogInfo(
                transactionServer.getLastProcessedCommitLogFileID,
                scheduledCommitLog.currentCommitLogFile)
              )
            )
          )(messageId, message.token, isFireAndForgetMethod))

          result match {
            case scala.util.Success(response) =>
              logSuccessfulProcession(Descriptors.GetCommitLogOffsets.name)
              sendResponseToClient(response, ctx, isFireAndForgetMethod)
            case scala.util.Failure(error) =>
              logUnsuccessfulProcessing(Descriptors.GetCommitLogOffsets.name, error)
              lazy val response = Descriptors.GetCommitLogOffsets.encodeResponseToMessage(TransactionService.GetCommitLogOffsets.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
              sendResponseToClient(response, ctx, isFireAndForgetMethod)
          }
        }
      }(serverReadContext)

      case Descriptors.PutStream.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.PutStream.encodeResponseToMessage(TransactionService.PutStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.PutStream.name, packageTooBigException)
          lazy val response = Descriptors.PutStream.encodeResponseToMessage(TransactionService.PutStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val args = Descriptors.PutStream.decodeRequest(message)
          val result = transactionServer.putStream(args.name, args.partitions, args.description, args.ttl)
          logSuccessfulProcession(Descriptors.PutStream.name)
          lazy val response = Descriptors.PutStream.encodeResponseToMessage(TransactionService.PutStream.Result(Some(result)))(messageId, message.token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverWriteContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutStream.name, error)
          lazy val response = Descriptors.PutStream.encodeResponseToMessage(TransactionService.PutStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverWriteContext)


      case Descriptors.CheckStreamExists.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.CheckStreamExists.encodeResponseToMessage(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.CheckStreamExists.name, packageTooBigException)
          lazy val response = Descriptors.CheckStreamExists.encodeResponseToMessage(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        } else {
          val args = Descriptors.CheckStreamExists.decodeRequest(message)
          val result = transactionServer.checkStreamExists(args.name)
          logSuccessfulProcession(Descriptors.CheckStreamExists.name)
          lazy val response = Descriptors.CheckStreamExists.encodeResponseToMessage(TransactionService.CheckStreamExists.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverReadContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.CheckStreamExists.name, error)
          lazy val response = Descriptors.CheckStreamExists.encodeResponseToMessage(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverReadContext)

      case Descriptors.GetStream.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.GetStream.encodeResponseToMessage(TransactionService.GetStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.GetStream.name, packageTooBigException)
          lazy val response = Descriptors.GetStream.encodeResponseToMessage(TransactionService.GetStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val args = Descriptors.GetStream.decodeRequest(message)
          val result = transactionServer.getStream(args.name)
          logSuccessfulProcession(Descriptors.GetStream.name)
          lazy val response = Descriptors.GetStream.encodeResponseToMessage(
            TransactionService.GetStream.Result(result)
          )(messageId, token, isFireAndForgetMethod)

          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverReadContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.GetStream.name, error)
          lazy val response = Descriptors.GetStream.encodeResponseToMessage(TransactionService.GetStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverReadContext)

      case Descriptors.DelStream.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.DelStream.encodeResponseToMessage(TransactionService.DelStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.DelStream.name, packageTooBigException)
          lazy val response = Descriptors.DelStream.encodeResponseToMessage(TransactionService.DelStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val args = Descriptors.DelStream.decodeRequest(message)
          val result = transactionServer.delStream(args.name)
          logSuccessfulProcession(Descriptors.DelStream.name)
          lazy val response = Descriptors.DelStream.encodeResponseToMessage(TransactionService.DelStream.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverWriteContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.DelStream.name, error)
          lazy val response = Descriptors.DelStream.encodeResponseToMessage(TransactionService.DelStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverWriteContext)


      case Descriptors.GetTransactionID.methodID =>
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.GetTransactionID.encodeResponse(TransactionService.GetTransactionID.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        } else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.GetTransactionID.name, packageTooBigException)
          lazy val response = Descriptors.GetTransactionID.encodeResponse(TransactionService.GetTransactionID.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        } else {
          val result = transactionServer.getTransactionID
          logSuccessfulProcession(Descriptors.GetTransactionID.name)
          lazy val response = Descriptors.GetTransactionID.encodeResponse(TransactionService.GetTransactionID.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }

      case Descriptors.GetTransactionIDByTimestamp.methodID =>
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.GetTransactionIDByTimestamp.encodeResponse(TransactionService.GetTransactionIDByTimestamp.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        } else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.GetTransactionIDByTimestamp.name, packageTooBigException)
          lazy val response = Descriptors.GetTransactionIDByTimestamp.encodeResponse(TransactionService.GetTransactionIDByTimestamp.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        } else {
          val args = Descriptors.GetTransactionIDByTimestamp.decodeRequest(message)
          val result = transactionServer.getTransactionIDByTimestamp(args.timestamp)
          logSuccessfulProcession(Descriptors.GetTransactionIDByTimestamp.name)
          lazy val response = Descriptors.GetTransactionIDByTimestamp.encodeResponse(TransactionService.GetTransactionIDByTimestamp.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }


      case Descriptors.PutTransaction.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.PutTransaction.encodeResponseToMessage(TransactionService.PutTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.PutTransaction.name, packageTooBigException)
          lazy val response = Descriptors.PutTransaction.encodeResponseToMessage(TransactionService.PutTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionType, message.body)
          logSuccessfulProcession(Descriptors.PutTransaction.name)
          lazy val response = Descriptors.PutTransaction.encodeResponseToMessage(TransactionService.PutTransaction.Result(Some(isPutted)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(commitLogContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutTransaction.name, error)
          lazy val response = Descriptors.PutTransaction.encodeResponseToMessage(TransactionService.PutTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(commitLogContext)


      case Descriptors.PutTransactions.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.PutTransactions.encodeResponseToMessage(TransactionService.PutTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.PutTransactions.name, packageTooBigException)
          lazy val response = Descriptors.PutTransactions.encodeResponseToMessage(TransactionService.PutTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, message.body)
          logSuccessfulProcession(Descriptors.PutTransactions.name)
          lazy val response = Descriptors.PutTransactions.encodeResponseToMessage(TransactionService.PutTransactions.Result(Some(isPutted)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(commitLogContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutTransactions.name, error)
          lazy val response = Descriptors.PutTransactions.encodeResponseToMessage(TransactionService.PutTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(commitLogContext)


      case Descriptors.PutSimpleTransactionAndData.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.PutSimpleTransactionAndData.encodeResponseToMessage(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.PutSimpleTransactionAndData.name, packageTooBigException)
          lazy val response = Descriptors.PutSimpleTransactionAndData.encodeResponseToMessage(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val txn = Descriptors.PutSimpleTransactionAndData.decodeRequest(message)
          transactionServer.putTransactionData(txn.streamID, txn.partition, txn.transaction, txn.data, 0)
          val transactions = collection.immutable.Seq(
            Transaction(Some(ProducerTransaction(txn.streamID, txn.partition, txn.transaction, TransactionStates.Opened, txn.data.size, 3L)), None),
            Transaction(Some(ProducerTransaction(txn.streamID, txn.partition, txn.transaction, TransactionStates.Checkpointed, txn.data.size, 120L)), None)
          )
          val messageForPutTransactions = Descriptors.PutTransactions.encodeRequest(TransactionService.PutTransactions.Args(transactions))
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, messageForPutTransactions)
          logSuccessfulProcession(Descriptors.PutSimpleTransactionAndData.name)
          lazy val response = Descriptors.PutSimpleTransactionAndData.encodeResponseToMessage(TransactionService.PutSimpleTransactionAndData.Result(Some(isPutted)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(commitLogContext).recover { case error =>
        logUnsuccessfulProcessing(Descriptors.PutSimpleTransactionAndData.name, error)
        lazy val response = Descriptors.PutSimpleTransactionAndData.encodeResponseToMessage(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
        sendResponseToClient(response, ctx, isFireAndForgetMethod)
      }(commitLogContext)


      case Descriptors.GetTransaction.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.GetTransaction.encodeResponseToMessage(TransactionService.GetTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.GetTransaction.name, packageTooBigException)
          lazy val response = Descriptors.GetTransaction.encodeResponseToMessage(TransactionService.GetTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val args = Descriptors.GetTransaction.decodeRequest(message)
          val result = transactionServer.getTransaction(args.streamID, args.partition, args.transaction)
          logSuccessfulProcession(Descriptors.GetTransaction.name)
          lazy val response = Descriptors.GetTransaction.encodeResponseToMessage(TransactionService.GetTransaction.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverReadContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.GetTransaction.name, error)
          lazy val response = Descriptors.GetTransaction.encodeResponseToMessage(TransactionService.GetTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverReadContext)


      case Descriptors.GetLastCheckpointedTransaction.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.GetLastCheckpointedTransaction.encodeResponseToMessage(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.GetLastCheckpointedTransaction.name, packageTooBigException)
          lazy val response = Descriptors.GetLastCheckpointedTransaction.encodeResponseToMessage(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val args = Descriptors.GetLastCheckpointedTransaction.decodeRequest(message)
          val result = transactionServer.getLastCheckpointedTransaction(args.streamID, args.partition)
          logSuccessfulProcession(Descriptors.GetLastCheckpointedTransaction.name)
          lazy val response = Descriptors.GetLastCheckpointedTransaction.encodeResponseToMessage(TransactionService.GetLastCheckpointedTransaction.Result(result))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverReadContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.GetLastCheckpointedTransaction.name, error)
          lazy val response = Descriptors.GetLastCheckpointedTransaction.encodeResponseToMessage(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverReadContext)

      case Descriptors.ScanTransactions.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.ScanTransactions.encodeResponseToMessage(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.ScanTransactions.name, packageTooBigException)
          lazy val response = Descriptors.ScanTransactions.encodeResponseToMessage(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val args = Descriptors.ScanTransactions.decodeRequest(message)
          val result = transactionServer.scanTransactions(args.streamID, args.partition, args.from, args.to, args.count, args.states)
          logSuccessfulProcession(Descriptors.ScanTransactions.name)
          lazy val response = Descriptors.ScanTransactions.encodeResponseToMessage(TransactionService.ScanTransactions.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverReadContext).recover { case error =>
          logUnsuccessfulProcessing(Descriptors.ScanTransactions.name, error)
          lazy val response = Descriptors.ScanTransactions.encodeResponseToMessage(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverWriteContext)

      case Descriptors.PutTransactionData.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.PutTransactionData.encodeResponseToMessage(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigDataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.PutTransactionData.name, packageTooBigException)
          lazy val response = Descriptors.PutTransactionData.encodeResponseToMessage(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val args = Descriptors.PutTransactionData.decodeRequest(message)
          val result = transactionServer.putTransactionData(args.streamID, args.partition, args.transaction, args.data, args.from)
          logSuccessfulProcession(Descriptors.PutTransactionData.name)
          lazy val response = Descriptors.PutTransactionData.encodeResponseToMessage(TransactionService.PutTransactionData.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverWriteContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutTransactionData.name, error)
          lazy val response = Descriptors.PutTransactionData.encodeResponseToMessage(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverWriteContext)

      case Descriptors.GetTransactionData.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.GetTransactionData.encodeResponseToMessage(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.GetTransactionData.name, packageTooBigException)
          lazy val response = Descriptors.GetTransactionData.encodeResponseToMessage(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val args = Descriptors.GetTransactionData.decodeRequest(message)
          val result = transactionServer.getTransactionData(args.streamID, args.partition, args.transaction, args.from, args.to)
          logSuccessfulProcession(Descriptors.GetTransactionData.name)
          lazy val response = Descriptors.GetTransactionData.encodeResponseToMessage(TransactionService.GetTransactionData.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverWriteContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.GetTransactionData.name, error)
          lazy val response = Descriptors.GetTransactionData.encodeResponseToMessage(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverWriteContext)


      case Descriptors.PutConsumerCheckpoint.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.PutConsumerCheckpoint.encodeResponseToMessage(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.PutConsumerCheckpoint.name, packageTooBigException)
          lazy val response = Descriptors.PutConsumerCheckpoint.encodeResponseToMessage(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.setConsumerStateType, message.body)
          logSuccessfulProcession(Descriptors.PutConsumerCheckpoint.name)
          lazy val response = Descriptors.PutConsumerCheckpoint.encodeResponseToMessage(TransactionService.PutConsumerCheckpoint.Result(Some(isPutted)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(commitLogContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutConsumerCheckpoint.name, error)
          lazy val response = Descriptors.PutConsumerCheckpoint.encodeResponseToMessage(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(commitLogContext)


      case Descriptors.GetConsumerState.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          lazy val response = Descriptors.GetConsumerState.encodeResponseToMessage(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else if (isTooBigMetadataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.GetConsumerState.name, packageTooBigException)
          lazy val response = Descriptors.GetConsumerState.encodeResponseToMessage(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
        else {
          val args = Descriptors.GetConsumerState.decodeRequest(message)
          val result = transactionServer.getConsumerState(args.name, args.streamID, args.partition)
          logSuccessfulProcession(Descriptors.GetConsumerState.name)
          lazy val response = Descriptors.GetConsumerState.encodeResponseToMessage(TransactionService.GetConsumerState.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }
      }(serverWriteContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.GetConsumerState.name, error)
          lazy val response = Descriptors.GetConsumerState.encodeResponseToMessage(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token, isFireAndForgetMethod)
          sendResponseToClient(response, ctx, isFireAndForgetMethod)
        }(serverWriteContext)

      case Descriptors.Authenticate.methodID =>
        val args = Descriptors.Authenticate.decodeRequest(message)
        val result = transactionServer.authenticate(args.authKey)
        val authInfo = AuthInfo(result,
          packageTransmissionOpts.maxMetadataPackageSize,
          packageTransmissionOpts.maxDataPackageSize
        )
        logSuccessfulProcession(Descriptors.Authenticate.name)
        val response = Descriptors.Authenticate.encodeResponseToMessage(TransactionService.Authenticate.Result(Some(authInfo)))(messageId, token, isFireAndForgetMethod)
        sendResponseToClient(response, ctx, isFireAndForgetMethod)


      case Descriptors.IsValid.methodID =>
        val args = Descriptors.IsValid.decodeRequest(message)
        val result = transactionServer.isValid(args.token)
        logSuccessfulProcession(Descriptors.IsValid.name)
        val response = Descriptors.IsValid.encodeResponseToMessage(TransactionService.IsValid.Result(Some(result)))(messageId, token, isFireAndForgetMethod)
        sendResponseToClient(response, ctx, isFireAndForgetMethod)
    }
  }
}