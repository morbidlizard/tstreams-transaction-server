package com.bwsw.tstreamstransactionserver.netty.server


import com.bwsw.tstreamstransactionserver.exception.Throwable.{PackageTooBigException, TokenInvalidException}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.CommitLogToBerkeleyWriter
import com.bwsw.tstreamstransactionserver.netty.server.handler.{RequestHandler, RequestHandlerChooser}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message}
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionService, TransactionStates}
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}

class ServerHandler(requestHandlerChooser: RequestHandlerChooser, logger: Logger)
  extends SimpleChannelInboundHandler[ByteBuf]
{
  private lazy val packageTooBigException = new PackageTooBigException(s"A size of client request is greater " +
    s"than maxMetadataPackageSize (${requestHandlerChooser.packageTransmissionOpts.maxMetadataPackageSize}) " +
    s"or maxDataPackageSize (${requestHandlerChooser.packageTransmissionOpts.maxDataPackageSize}).")

  private val serverWriteContext: ExecutionContext = requestHandlerChooser.server.executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext = requestHandlerChooser.server.executionContext.serverReadContext

  private def isTooBigMetadataMessage(message: Message) = {
    message.length > requestHandlerChooser.packageTransmissionOpts.maxMetadataPackageSize
  }

  private def isTooBigDataMessage(message: Message) = {
    message.length > requestHandlerChooser.packageTransmissionOpts.maxDataPackageSize
  }

  @volatile var isChannelActive = true
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    isChannelActive = false
    //    if (logger.isInfoEnabled) logger.info(s"${ctx.channel().remoteAddress().toString} is inactive")
    ctx.fireChannelInactive()
  }

  @inline
  private def sendResponseToClient(message: Message, ctx: ChannelHandlerContext): Unit = {
    val binaryResponse = message.toByteArray
    if (isChannelActive)
      ctx.writeAndFlush(binaryResponse)
  }

  private val commitLogContext = requestHandlerChooser.server.executionContext.commitLogContext


  private def logSuccessfulProcession(method: String, message: Message, ctx: ChannelHandlerContext): Unit =
    if (logger.isDebugEnabled)
      logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}: $method is successfully processed!")

  private def logUnsuccessfulProcessing(method: String, error: Throwable, message: Message, ctx: ChannelHandlerContext): Unit =
    if (logger.isDebugEnabled)
      logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}:  $method is failed while processing!", error)


  private def processRequestAsync(context: ExecutionContext,
                                  handler: RequestHandler,
                                  isTooBigMessage: Message => Boolean,
                                  ctx: ChannelHandlerContext
                                 )(message: Message) =
    ScalaFuture {
      processRequest(handler, isTooBigMessage, ctx)(message)
    }(context)
    .recover { case error =>
      logUnsuccessfulProcessing(handler.getName, error, message, ctx)
      val response = handler.createErrorResponse(error.getMessage)
      val responseMessage = message.copy(length = response.length, body = response)
      sendResponseToClient(responseMessage, ctx)
    }(context)

  private def processRequest(handler: RequestHandler,
                             isTooBigMessage: Message => Boolean,
                             ctx: ChannelHandlerContext
                            )(message: Message) = {
    if (!requestHandlerChooser.server.isValid(message.token)) {
      val response = handler.createErrorResponse(
        com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage
      )
      val responseMessage  = message.copy(length = response.length, body = response)
      sendResponseToClient(responseMessage, ctx)
    }
    else if (isTooBigMessage(message)) {
      logUnsuccessfulProcessing(handler.getName, packageTooBigException, message, ctx)
      val response = handler.createErrorResponse(
        packageTooBigException.getMessage
      )
      val responseMessage  = message.copy(length = response.length, body = response)
      sendResponseToClient(responseMessage, ctx)
    }
    else {
      val response = handler.handleAndGetResponse(message.body)
      val responseMessage  = message.copy(length = response.length, body = response)

      logSuccessfulProcession(handler.getName, message, ctx)
      sendResponseToClient(responseMessage, ctx)
    }
  }

  private def processFutureRequest(handler: RequestHandler,
                                   isTooBigMessage: Message => Boolean,
                                   ctx: ChannelHandlerContext,
                                   f: => ScalaFuture[Unit]
                                  )(message: Message) = {
    if (!requestHandlerChooser.server.isValid(message.token)) {
      val response = handler.createErrorResponse(
        com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage
      )
      val responseMessage  = message.copy(length = response.length, body = response)
      sendResponseToClient(responseMessage, ctx)
    }
    else if (isTooBigMessage(message)) {
      logUnsuccessfulProcessing(handler.getName, packageTooBigException, message, ctx)
      val response = handler.createErrorResponse(
        packageTooBigException.getMessage
      )
      val responseMessage  = message.copy(length = response.length, body = response)
      sendResponseToClient(responseMessage, ctx)
    }
    else {
      f
    }
  }

  private def processFutureRequestFireAndForget(handler: RequestHandler,
                                                isTooBigMessage: Message => Boolean,
                                                ctx: ChannelHandlerContext,
                                                f: => ScalaFuture[Unit]
                                               )(message: Message) =

      if (!requestHandlerChooser.server.isValid(message.token)) {
        logUnsuccessfulProcessing(handler.getName, new TokenInvalidException(), message, ctx)
      }
      else if (isTooBigMessage(message)) {
        logUnsuccessfulProcessing(handler.getName, packageTooBigException, message, ctx)
      }
      else
        f


  private def processRequestAsyncFireAndForget(context: ExecutionContext,
                                               handler: RequestHandler,
                                               isTooBigMessage: Message => Boolean,
                                               ctx: ChannelHandlerContext
                                              )(message: Message) =
    ScalaFuture {
      if (!requestHandlerChooser.server.isValid(message.token)) {
        logUnsuccessfulProcessing(handler.getName, new TokenInvalidException(), message, ctx)
      }
      else if (isTooBigMessage(message)) {
        logUnsuccessfulProcessing(handler.getName, packageTooBigException, message, ctx)
      }
      else
        handler.handle(message.body)
    }(context)


  private val orderedExecutionPool = requestHandlerChooser.orderedExecutionPool
  private val subscriberNotifier = requestHandlerChooser.openTransactionStateNotifier
  private def processRequestAndReplyClient(handler: RequestHandler,
                                           message: Message,
                                           ctx: ChannelHandlerContext
                                          ): Unit = {
    message.method match {
      case Descriptors.GetCommitLogOffsets.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutStream.methodID =>
        processRequestAsync(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.CheckStreamExists.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.GetStream.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.DelStream.methodID =>
        processRequestAsync(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.GetTransactionID.methodID =>
        processRequest(handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.GetTransactionIDByTimestamp.methodID =>
        processRequest(handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutTransaction.methodID =>
        processRequestAsync(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutTransactions.methodID =>
        processRequestAsync(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutProducerStateWithData.methodID =>
        processRequestAsync(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.OpenTransaction.methodID =>
        processFutureRequest(handler, isTooBigMetadataMessage, ctx, {
          val args = Descriptors.OpenTransaction.decodeRequest(message.body)
          val context = orderedExecutionPool.pool(args.streamID, args.partition)
          ScalaFuture {
            val transactionID =
              requestHandlerChooser.server.getTransactionID

            val txn = Transaction(Some(
              ProducerTransaction(
                args.streamID,
                args.partition,
                transactionID,
                TransactionStates.Opened,
                quantity = 0,
                ttl = args.transactionTTLMs
              )), None
            )

            val binaryTransaction = Descriptors.PutTransaction.encodeRequest(
              TransactionService.PutTransaction.Args(txn)
            )

            requestHandlerChooser.scheduledCommitLog.putData(
              CommitLogToBerkeleyWriter.putTransactionType,
              binaryTransaction
            )

            val response = Descriptors.OpenTransaction.encodeResponse(
              TransactionService.OpenTransaction.Result(
                Some(transactionID)
              )
            )

            val responseMessage = message.copy(length = response.length, body = response)

            logSuccessfulProcession(handler.getName, message, ctx)
            sendResponseToClient(responseMessage, ctx)

            subscriberNotifier.notifySubscribers(
              args.streamID,
              args.partition,
              transactionID,
              count = 0,
              TransactionState.Status.Opened,
              args.transactionTTLMs,
              requestHandlerChooser.authOptions.key,
              isNotReliable = false
            )
          }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(handler.getName, error, message, ctx)
              val response = handler.createErrorResponse(error.getMessage)
              val responseMessage = message.copy(length = response.length, body = response)
              sendResponseToClient(responseMessage, ctx)
            }(context)
        })(message)

      case Descriptors.PutSimpleTransactionAndData.methodID =>
        processFutureRequest(handler, isTooBigMetadataMessage, ctx, {
          val txn = Descriptors.PutSimpleTransactionAndData.decodeRequest(message.body)
          val context = orderedExecutionPool.pool(txn.streamID, txn.partition)
          ScalaFuture {
            val transactionID = requestHandlerChooser.server
              .getTransactionID

            requestHandlerChooser.server.putTransactionData(
              txn.streamID,
              txn.partition,
              transactionID,
              txn.data,
              0
            )

            val transactions = collection.immutable.Seq(
              Transaction(Some(
                ProducerTransaction(
                  txn.streamID,
                  txn.partition,
                  transactionID,
                  TransactionStates.Opened,
                  txn.data.size, 3000L
                )), None
              ),
              Transaction(Some(
                ProducerTransaction(
                  txn.streamID,
                  txn.partition,
                  transactionID,
                  TransactionStates.Checkpointed,
                  txn.data.size,
                  Long.MaxValue)), None
              )
            )
            val messageForPutTransactions =
              Descriptors.PutTransactions.encodeRequest(
                TransactionService.PutTransactions.Args(transactions)
              )


            requestHandlerChooser.scheduledCommitLog.putData(
              CommitLogToBerkeleyWriter.putTransactionsType,
              messageForPutTransactions
            )

            val response = Descriptors.PutSimpleTransactionAndData.encodeResponse(
              TransactionService.PutSimpleTransactionAndData.Result(
                Some(transactionID)
              )
            )

            val responseMessage  = message.copy(length = response.length, body = response)

            logSuccessfulProcession(handler.getName, message, ctx)
            sendResponseToClient(responseMessage, ctx)

            subscriberNotifier.notifySubscribers(
              txn.streamID,
              txn.partition,
              transactionID,
              txn.data.size,
              TransactionState.Status.Instant,
              Long.MaxValue,
              requestHandlerChooser.authOptions.key,
              isNotReliable = false
            )

          }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(handler.getName, error, message, ctx)
              val response = handler.createErrorResponse(error.getMessage)
              val responseMessage = message.copy(length = response.length, body = response)
              sendResponseToClient(responseMessage, ctx)
            }(context)
        })(message)

      case Descriptors.GetTransaction.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.GetLastCheckpointedTransaction.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.ScanTransactions.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutTransactionData.methodID =>
        processRequestAsync(serverWriteContext, handler, isTooBigDataMessage, ctx)(message)

      case Descriptors.GetTransactionData.methodID =>
        processRequestAsync(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutConsumerCheckpoint.methodID =>
        processRequestAsync(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.GetConsumerState.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.Authenticate.methodID =>
        val response = handler.handleAndGetResponse(message.body)
        val responseMessage = message.copy(length = response.length, body = response)
        logSuccessfulProcession(handler.getName, message, ctx)
        sendResponseToClient(responseMessage, ctx)

      case Descriptors.IsValid.methodID =>
        val response = handler.handleAndGetResponse(message.body)
        val responseMessage = message.copy(length = response.length, body = response)
        logSuccessfulProcession(handler.getName, message, ctx)
        sendResponseToClient(responseMessage, ctx)
    }
  }

  private def processRequestFireAndForgetManner(handler: RequestHandler,
                                                message: Message,
                                                ctx: ChannelHandlerContext
                                               ): Unit =
  {
    message.method match {
      case Descriptors.PutStream.methodID =>
        processRequestAsyncFireAndForget(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.DelStream.methodID =>
        processRequestAsyncFireAndForget(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutTransaction.methodID =>
        processRequestAsyncFireAndForget(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutTransactions.methodID =>
        processRequestAsyncFireAndForget(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutProducerStateWithData.methodID =>
        processRequestAsyncFireAndForget(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Descriptors.PutSimpleTransactionAndData.methodID =>
        processFutureRequestFireAndForget(handler, isTooBigMetadataMessage, ctx, {
          val txn = Descriptors.PutSimpleTransactionAndData.decodeRequest(message.body)
          val context = orderedExecutionPool.pool(txn.streamID, txn.partition)
          ScalaFuture {

            val transactionID =
              requestHandlerChooser.server.getTransactionID

            requestHandlerChooser.server.putTransactionData(
              txn.streamID,
              txn.partition,
              transactionID,
              txn.data,
              0
            )

            val transactions = collection.immutable.Seq(
              Transaction(Some(
                ProducerTransaction(
                  txn.streamID,
                  txn.partition,
                  transactionID,
                  TransactionStates.Opened,
                  txn.data.size, 3000L
                )), None
              ),
              Transaction(Some(
                ProducerTransaction(
                  txn.streamID,
                  txn.partition,
                  transactionID,
                  TransactionStates.Checkpointed,
                  txn.data.size,
                  Long.MaxValue)), None
              )
            )
            val messageForPutTransactions =
              Descriptors.PutTransactions.encodeRequest(
                TransactionService.PutTransactions.Args(transactions)
              )

            requestHandlerChooser.scheduledCommitLog.putData(
              CommitLogToBerkeleyWriter.putTransactionsType,
              messageForPutTransactions
            )

            logSuccessfulProcession(handler.getName, message, ctx)

            subscriberNotifier.notifySubscribers(
              txn.streamID,
              txn.partition,
              transactionID,
              txn.data.size,
              TransactionState.Status.Instant,
              Long.MaxValue,
              requestHandlerChooser.authOptions.key,
              isNotReliable = true
            )

          }(context)
        })(message)

      case Descriptors.PutTransactionData.methodID =>
        processRequestAsyncFireAndForget(serverWriteContext, handler, isTooBigDataMessage, ctx)(message)

      case Descriptors.PutConsumerCheckpoint.methodID =>
        processRequestAsyncFireAndForget(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case _ =>
        ()
    }

  }

  override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
    val message = Message.fromByteBuf(buf)
    invokeMethod(message, ctx)
  }

  protected def invokeMethod(message: Message, ctx: ChannelHandlerContext): Unit = {

    if (logger.isDebugEnabled)
      logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id} method is invoked.")

    val isFireAndForgetMethod: Boolean =
      if (message.isFireAndForgetMethod == (1:Byte))
        true
      else
        false

    val handler = requestHandlerChooser.handler(message.method)
    if (isFireAndForgetMethod)
      processRequestFireAndForgetManner(handler, message, ctx)
    else
      processRequestAndReplyClient(handler, message, ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.channel().close()
    // ctx.channel().parent().close()
  }
}