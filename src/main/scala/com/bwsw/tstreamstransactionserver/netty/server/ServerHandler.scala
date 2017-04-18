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

    def logSuccessfulProcession(): Unit = if (logger.isDebugEnabled) logger.debug(s"$inetAddress request id ${message.id}: METHOD is successfully processed!")
    def logUnsuccessfulProcessing(error: Throwable): Unit = if (logger.isDebugEnabled) logger.debug(s"$inetAddress request id ${message.id}: METHOD is failed while processing!", error)


//    if (logger.isDebugEnabled) logger.debug(s"$inetAddress request id ${message.id}: $method is invoked.")

    message.method match {
      case Descriptors.GetCommitLogOffsets.methodID =>
        if (transactionServer.isValid(message.token)) {
          ScalaFuture((scheduledCommitLog.currentCommitLogFile, transactionServer.getLastProcessedCommitLogFileID))
            .map { case (currentCommitLogFileID, lastProcessedCommitLogFileID) =>
              logSuccessfulProcession()
              Descriptors.GetCommitLogOffsets.encodeResponse(
                TransactionService.GetCommitLogOffsets.Result(
                  Some(CommitLogInfo(lastProcessedCommitLogFileID.getOrElse(-1L), currentCommitLogFileID))
                )
              )(messageId, message.token)
            }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(error)
              Descriptors.GetCommitLogOffsets.encodeResponse(TransactionService.GetCommitLogOffsets.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
            }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetCommitLogOffsets.encodeResponse(TransactionService.GetCommitLogOffsets.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.PutStream.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(Descriptors.PutStream.decodeRequest(message))
              .flatMap{args =>
                transactionServer.putStream(args.stream, args.partitions, args.description, args.ttl)
                  .map { response =>
                    logSuccessfulProcession()
                    Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(Some(response)))(messageId, message.token)
                  }(context)
                  .recover { case error =>
                    logUnsuccessfulProcessing(error)
                    Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                  }}(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }


      case Descriptors.CheckStreamExists.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(Descriptors.CheckStreamExists.decodeRequest(message))
              .flatMap(args =>
                transactionServer.checkStreamExists(args.stream)
                  .map { response =>
                    logSuccessfulProcession()
                    Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(Some(response)))(messageId, token)
                  }(context)
                  .recover { case error =>
                    logUnsuccessfulProcessing(error)
                    Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                  })(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.CheckStreamExists.encodeResponse(TransactionService.CheckStreamExists.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }


      case Descriptors.GetStream.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(Descriptors.GetStream.decodeRequest(message))
              .flatMap(args =>
                transactionServer.getStream(args.stream)
                  .map { response =>
                    logSuccessfulProcession()
                    Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(Some(response)))(messageId, token)
                  }(context)
                  .recover { case error =>
                    logUnsuccessfulProcessing(error)
                    Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                  })(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }


      case Descriptors.DelStream.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(Descriptors.DelStream.decodeRequest(message))
              .flatMap(args =>
                transactionServer.delStream(args.stream)
                  .map { response =>
                    logSuccessfulProcession()
                    Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(Some(response)))(messageId, token)
                  }(context)
                  .recover { case error =>
                    logUnsuccessfulProcessing(error)
                    Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                  })(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.PutTransaction.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionType, message))(commitLogContext)
              .map { isOkay =>
                logSuccessfulProcession()
                Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(Some(isOkay)))(messageId, token)
              }(context)
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.PutTransactions.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, message))(commitLogContext)
              .map { isOkay =>
                logSuccessfulProcession()
                Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(Some(isOkay)))(messageId, token)
              }(context)
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.PutSimpleTransactionAndData.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(Descriptors.PutSimpleTransactionAndData.decodeRequest(message))(commitLogContext)
              .flatMap { txn =>
                ScalaFuture(transactionServer.putTransactionDataSync(txn.stream, txn.partition, txn.transaction, txn.data, txn.from))(commitLogContext)
                  .map { _ =>
                    val transactions = Seq(
                      Transaction(Some(ProducerTransaction(txn.stream, txn.partition, txn.transaction, TransactionStates.Opened, txn.data.size, 3L)), None),
                      Transaction(Some(ProducerTransaction(txn.stream, txn.partition, txn.transaction, TransactionStates.Checkpointed, txn.data.size, 120L)), None)
                    )
                    Descriptors.PutTransactions.encodeRequest(TransactionService.PutTransactions.Args(transactions))(messageId, token)
                  }(commitLogContext)
              }(commitLogContext)
              .flatMap(messageForPutTransactions =>
                ScalaFuture(scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, messageForPutTransactions))(commitLogContext)
                  .map { isOkay =>
                    logSuccessfulProcession()
                    Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(Some(isOkay)))(messageId, token)
                  }(commitLogContext)
                  .recover { case error =>
                    logUnsuccessfulProcessing(error)
                    Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                  })(commitLogContext)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.GetTransaction.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(Descriptors.GetTransaction.decodeRequest(message))
              .flatMap(args =>
                transactionServer.getTransaction(args.stream, args.partition, args.transaction)
                  .flatMap { response =>
                    logSuccessfulProcession()
                    ScalaFuture.successful(Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(Some(response)))(messageId, token))
                  }(context)
                  .recover { case error =>
                    logUnsuccessfulProcessing(error)
                    Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                  })(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetTransaction.encodeResponse(TransactionService.GetTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.GetLastCheckpointedTransaction.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(Descriptors.GetLastCheckpointedTransaction.decodeRequest(message)).flatMap(args =>
              transactionServer.getLastCheckpointedTransaction(args.stream, args.partition)
                .flatMap { response =>
                  logSuccessfulProcession()
                  ScalaFuture.successful(Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(response))(messageId, token))
                }(context)
                .recover { case error =>
                  logUnsuccessfulProcessing(error)
                  Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                })(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetLastCheckpointedTransaction.encodeResponse(TransactionService.GetLastCheckpointedTransaction.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.ScanTransactions.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(Descriptors.ScanTransactions.decodeRequest(message)).flatMap(args =>
              transactionServer.scanTransactions(args.stream, args.partition, args.from, args.to, ObjectSerializer.deserialize(args.lambda).asInstanceOf[ProducerTransaction => Boolean])
                .flatMap { response =>
                  logSuccessfulProcession()
                  ScalaFuture.successful(Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(Some(response)))(messageId, token))
                }(context)
                .recover { case error =>
                  logUnsuccessfulProcessing(error)
                  Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                })(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.PutTransactionData.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigDataMessage(message)) {
            ScalaFuture(Descriptors.PutTransactionData.decodeRequest(message))
              .flatMap(args =>
                transactionServer.putTransactionData(args.stream, args.partition, args.transaction, args.data, args.from)
                  .flatMap { response =>
                    logSuccessfulProcession()
                    ScalaFuture.successful(Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(Some(response)))(messageId, token))
                  }(context)
                  .recover { case error =>
                    logUnsuccessfulProcessing(error)
                    Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                  })(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }


      case Descriptors.GetTransactionData.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(Descriptors.GetTransactionData.decodeRequest(message))
              .flatMap(args =>
                transactionServer.getTransactionData(args.stream, args.partition, args.transaction, args.from, args.to)
                  .flatMap { response =>
                    logSuccessfulProcession()
                    ScalaFuture.successful(Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(Some(response)))(messageId, token))
                  }(context)
                  .recover { case error =>
                    logUnsuccessfulProcessing(error)
                    Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                  })(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }


      case Descriptors.PutConsumerCheckpoint.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture(scheduledCommitLog.putData(CommitLogToBerkeleyWriter.setConsumerStateType, message))(commitLogContext)
              .map { response =>
                logSuccessfulProcession()
                Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(Some(response)))(messageId, token)
              }(context)
              .recover { case error =>
                logUnsuccessfulProcessing(error)
                Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
              }(context)
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.PutConsumerCheckpoint.encodeResponse(TransactionService.PutConsumerCheckpoint.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.GetConsumerState.methodID =>
        if (transactionServer.isValid(message.token)) {
          if (!isTooBigPackage) {
            ScalaFuture {
              Descriptors.GetConsumerState.decodeRequest(message)
            } flatMap (args =>
              transactionServer.getConsumerState(args.name, args.stream, args.partition)
                .flatMap {
                  logSuccessfulProcession()
                  response => ScalaFuture.successful(Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(Some(response)))(messageId, token))
                }
                .recover { case error =>
                  logUnsuccessfulProcessing(error)
                  Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(error.getMessage))))(messageId, token)
                })
          } else ScalaFuture {
            logUnsuccessfulProcessing(packageTooBigException)
            Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(packageTooBigException.getMessage))))(messageId, token)
          }(context)
        } else {
          //logUnsuccessfulProcessing()
          ScalaFuture.successful(Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, error = Some(ServerException(com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage))))(messageId, token))
        }

      case Descriptors.Authenticate.methodID => ScalaFuture {
        val args = Descriptors.Authenticate.decodeRequest(message)
        val response = transactionServer.authenticate(args.authKey)
        val authInfo = AuthInfo(response, packageTransmissionOpts.maxMetadataPackageSize, packageTransmissionOpts.maxDataPackageSize)
        logSuccessfulProcession()
        Descriptors.Authenticate.encodeResponse(TransactionService.Authenticate.Result(Some(authInfo)))(messageId, token)
      }


      case Descriptors.IsValid.methodID => ScalaFuture {
        val args = Descriptors.IsValid.decodeRequest(message)
        val response = transactionServer.isValid(args.token)
        logSuccessfulProcession()
        Descriptors.IsValid.encodeResponse(TransactionService.IsValid.Result(Some(response)))(messageId, token)
      }
    }
  }
}