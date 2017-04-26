package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message, ObjectSerializer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc._
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}
import scala.util.Try

class ServerHandler(transactionServer: TransactionServer, scheduledCommitLog: ScheduledCommitLog, packageTransmissionOpts: TransportOptions, logger: Logger) extends SimpleChannelInboundHandler[ByteBuf] {
  private val packageTooBigException = new PackageTooBigException(s"A size of client request is greater " +
    s"than maxMetadataPackageSize (${packageTransmissionOpts.maxMetadataPackageSize}) or maxDataPackageSize (${packageTransmissionOpts.maxDataPackageSize}).")

  private val context: ExecutionContext = transactionServer.executionContext.context
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
  private def sendResponseToClient(message: Message, ctx: ChannelHandlerContext): Unit = {
    val binaryResponse = message.toByteArray
    if (isChannelActive) ctx.writeAndFlush(binaryResponse)
  }

  private val commitLogContext = transactionServer.executionContext.commitLogContext
  protected def invokeMethod(message: Message, ctx: ChannelHandlerContext): Unit = {

    implicit val (messageId: Long, token) = (message.id, message.token)
    def isTooBigPackage = isTooBigMetadataMessage(message)

    def logSuccessfulProcession(method: String): Unit = if (logger.isDebugEnabled) logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}: $method is successfully processed!")
    def logUnsuccessfulProcessing(method: String, error: Throwable): Unit = if (logger.isDebugEnabled) logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}:  $method is failed while processing!", error)

    if (logger.isDebugEnabled) logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id} with undefined yet method is invoked.")

    message.method match {

      case Descriptors.GetCommitLogOffsets.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.GetCommitLogOffsets.encodeResponse(TransactionService.GetCommitLogOffsets.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        } else {
          val result: Try[Message] = scala.util.Try(Descriptors.GetCommitLogOffsets.encodeResponse(
            TransactionService.GetCommitLogOffsets.Result(
              Some(CommitLogInfo(
                transactionServer.getLastProcessedCommitLogFileID.getOrElse(-1L),
                scheduledCommitLog.currentCommitLogFile)
              )
            )
          )(messageId, message.token))

          result match {
            case scala.util.Success(response) =>
              logSuccessfulProcession(Descriptors.GetCommitLogOffsets.name)
              sendResponseToClient(response, ctx)
            case scala.util.Failure(error) =>
              logUnsuccessfulProcessing(Descriptors.GetCommitLogOffsets.name, error)
              val response = Descriptors.GetCommitLogOffsets.encodeResponse(TransactionService.GetCommitLogOffsets.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
          }
        }
      }(context)

      case Descriptors.PutStream.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.PutStream.name, packageTooBigException)
          val response = Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val args = Descriptors.PutStream.decodeRequest(message)
          transactionServer.putStream(args.stream, args.partitions, args.description, args.ttl)
            .map { result =>
              logSuccessfulProcession(Descriptors.PutStream.name)
              val response = Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(Some(result)))(messageId, message.token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.PutStream.name, error)
              val response = Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)


      case Descriptors.CheckStreamExists.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.CheckStreamExists.name, packageTooBigException)
          val response = Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        } else {
          val args = Descriptors.CheckStreamExists.decodeRequest(message)
          transactionServer.checkStreamExists(args.stream)
            .map { result =>
              logSuccessfulProcession(Descriptors.CheckStreamExists.name)
              val response = Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(Some(result)))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.CheckStreamExists.name, error)
              val response = Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)

      case Descriptors.GetStream.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.GetStream.name, packageTooBigException)
          val response = Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val args = Descriptors.GetStream.decodeRequest(message)
          transactionServer.getStream(args.stream)
            .map { result =>
              logSuccessfulProcession(Descriptors.GetStream.name)
              val response = Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(Some(result)))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.GetStream.name, error)
              val response = Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)

      case Descriptors.DelStream.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.DelStream.name, packageTooBigException)
          val response = Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val args = Descriptors.DelStream.decodeRequest(message)
          transactionServer.delStream(args.stream)
            .map { result =>
              logSuccessfulProcession(Descriptors.DelStream.name)
              val response = Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(Some(result)))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.DelStream.name, error)
              val response = Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)

      case Descriptors.PutTransaction.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.PutTransaction.name, packageTooBigException)
          val response = Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionType, message)
          logSuccessfulProcession(Descriptors.PutTransaction.name)
          val response = Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(Some(isPutted)))(messageId, token)
          sendResponseToClient(response, ctx)
        }
      }(commitLogContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutTransaction.name, error)
          val response = Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }(commitLogContext)


      case Descriptors.PutTransactions.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.PutTransactions.name, packageTooBigException)
          val response = Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, message)
          logSuccessfulProcession(Descriptors.PutTransactions.name)
          val response = Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(Some(isPutted)))(messageId, token)
          sendResponseToClient(response, ctx)
        }
      }(commitLogContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutTransactions.name, error)
          val response = Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }(commitLogContext)


      case Descriptors.PutSimpleTransactionAndData.methodID => ScalaFuture{
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.PutSimpleTransactionAndData.name, packageTooBigException)
          val response = Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val txn = Descriptors.PutSimpleTransactionAndData.decodeRequest(message)
          transactionServer.putTransactionDataSync(txn.stream, txn.partition, txn.transaction, txn.data, txn.from)
          val transactions = collection.immutable.Seq(
            Transaction(Some(ProducerTransaction(txn.stream, txn.partition, txn.transaction, TransactionStates.Opened, txn.data.size, 3L)), None),
            Transaction(Some(ProducerTransaction(txn.stream, txn.partition, txn.transaction, TransactionStates.Checkpointed, txn.data.size, 120L)), None)
          )
          val messageForPutTransactions = Descriptors.PutTransactions.encodeRequest(TransactionService.PutTransactions.Args(transactions))(messageId, token)
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, messageForPutTransactions)
          logSuccessfulProcession(Descriptors.PutSimpleTransactionAndData.name)
          val response = Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(Some(isPutted)))(messageId, token)
          sendResponseToClient(response, ctx)
        }
      }(commitLogContext).recover { case error =>
        logUnsuccessfulProcessing(Descriptors.PutSimpleTransactionAndData.name, error)
        val response = Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
        sendResponseToClient(response, ctx)
      }(commitLogContext)


      case Descriptors.GetTransaction.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.GetTransaction.name, packageTooBigException)
          val response = Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val args = Descriptors.GetTransaction.decodeRequest(message)
          transactionServer.getTransaction(args.stream, args.partition, args.transaction)
            .map { result =>
              logSuccessfulProcession(Descriptors.GetTransaction.name)
              val response = Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(Some(result)))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.GetTransaction.name, error)
              val response = Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)


      case Descriptors.GetLastCheckpointedTransaction.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.GetLastCheckpointedTransaction.name, packageTooBigException)
          val response = Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val args = Descriptors.GetLastCheckpointedTransaction.decodeRequest(message)
          transactionServer.getLastCheckpointedTransaction(args.stream, args.partition)
            .map { result =>
              logSuccessfulProcession(Descriptors.GetLastCheckpointedTransaction.name)
              val response = Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(result))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.GetLastCheckpointedTransaction.name, error)
              val response = Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)

      case Descriptors.ScanTransactions.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.ScanTransactions.name, packageTooBigException)
          val response = Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val args = Descriptors.ScanTransactions.decodeRequest(message)
          transactionServer.scanTransactions(args.stream, args.partition, args.from, args.to, ObjectSerializer.deserialize(args.lambda).asInstanceOf[ProducerTransaction => Boolean])
            .map { result =>
              logSuccessfulProcession(Descriptors.ScanTransactions.name)
              val response = Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(Some(result)))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.ScanTransactions.name, error)
              val response = Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)

      case Descriptors.PutTransactionData.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigDataMessage(message)) {
          logUnsuccessfulProcessing(Descriptors.PutTransactionData.name, packageTooBigException)
          val response = Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val args = Descriptors.PutTransactionData.decodeRequest(message)
          transactionServer.putTransactionData(args.stream, args.partition, args.transaction, args.data, args.from)
            .map { result =>
              logSuccessfulProcession(Descriptors.PutTransactionData.name)
              val response = Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(Some(result)))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.PutTransactionData.name, error)
              val response = Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)

      case Descriptors.GetTransactionData.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.GetTransactionData.name, packageTooBigException)
          val response = Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val args = Descriptors.GetTransactionData.decodeRequest(message)
          transactionServer.getTransactionData(args.stream, args.partition, args.transaction, args.from, args.to)
            .map { result =>
              logSuccessfulProcession(Descriptors.GetTransactionData.name)
              val response = Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(Some(result)))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.GetTransactionData.name, error)
              val response = Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)


      case Descriptors.PutConsumerCheckpoint.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage) {
          logUnsuccessfulProcessing(Descriptors.PutConsumerCheckpoint.name, packageTooBigException)
          val response = Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.setConsumerStateType, message)
          logSuccessfulProcession(Descriptors.PutConsumerCheckpoint.name)
          val response = Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(Some(isPutted)))(messageId, token)
          sendResponseToClient(response, ctx)
        }
      }(commitLogContext)
        .recover { case error =>
          logUnsuccessfulProcessing(Descriptors.PutConsumerCheckpoint.name, error)
          val response = Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }(commitLogContext)



      case Descriptors.GetConsumerState.methodID => ScalaFuture {
        if (!transactionServer.isValid(message.token)) {
          val response = Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else if (isTooBigPackage)  {
          logUnsuccessfulProcessing(Descriptors.GetConsumerState.name, packageTooBigException)
          val response = Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          sendResponseToClient(response, ctx)
        }
        else {
          val args = Descriptors.GetConsumerState.decodeRequest(message)
          transactionServer.getConsumerState(args.name, args.stream, args.partition)
            .map {result =>
              logSuccessfulProcession(Descriptors.GetConsumerState.name)
              val response = Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(Some(result)))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(Descriptors.GetConsumerState.name, error)
              val response = Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              sendResponseToClient(response, ctx)
            }(context)
        }
      }(context)

      case Descriptors.Authenticate.methodID =>
        val args = Descriptors.Authenticate.decodeRequest(message)
        val result = transactionServer.authenticate(args.authKey)
        val authInfo = AuthInfo(result, packageTransmissionOpts.maxMetadataPackageSize, packageTransmissionOpts.maxDataPackageSize)
        logSuccessfulProcession(Descriptors.Authenticate.name)
        val response =  Descriptors.Authenticate.encodeResponse(TransactionService.Authenticate.Result(Some(authInfo)))(messageId, token)
        sendResponseToClient(response, ctx)



      case Descriptors.IsValid.methodID =>
        val args = Descriptors.IsValid.decodeRequest(message)
        val result = transactionServer.isValid(args.token)
        logSuccessfulProcession(Descriptors.IsValid.name)
        val response = Descriptors.IsValid.encodeResponse(TransactionService.IsValid.Result(Some(result)))(messageId, token)
        sendResponseToClient(response, ctx)
    }
  }
}