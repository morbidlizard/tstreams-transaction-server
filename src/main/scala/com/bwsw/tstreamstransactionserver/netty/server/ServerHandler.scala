/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.server


import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.exception.Throwable.{PackageTooBigException, TokenInvalidException}
import com.bwsw.tstreamstransactionserver.netty.server.handler.{RequestHandler, RequestHandlerRouter}
import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionService, TransactionStates}
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}


class ServerHandler(requestHandlerRouter: RequestHandlerRouter,
                    executionContext:ServerExecutionContextGrids,
                    logger: Logger)
  extends SimpleChannelInboundHandler[ByteBuf]
{
  private lazy val packageTooBigException = new PackageTooBigException(s"A size of client request is greater " +
    s"than maxMetadataPackageSize (${requestHandlerRouter.packageTransmissionOpts.maxMetadataPackageSize}) " +
    s"or maxDataPackageSize (${requestHandlerRouter.packageTransmissionOpts.maxDataPackageSize}).")

  private val serverWriteContext: ExecutionContext =
    executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext =
    executionContext.serverReadContext


  private def isTooBigMetadataMessage(message: Message) = {
    message.bodyLength > requestHandlerRouter.packageTransmissionOpts.maxMetadataPackageSize
  }

  private def isTooBigDataMessage(message: Message) = {
    message.bodyLength > requestHandlerRouter.packageTransmissionOpts.maxDataPackageSize
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

  private val commitLogContext = executionContext.commitLogContext

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
      logUnsuccessfulProcessing(handler.name, error, message, ctx)
      val response = handler.createErrorResponse(error.getMessage)
      val responseMessage = message.copy(bodyLength = response.length, body = response)
      sendResponseToClient(responseMessage, ctx)
    }(context)

  private def processRequest(handler: RequestHandler,
                             isTooBigMessage: Message => Boolean,
                             ctx: ChannelHandlerContext
                            )(message: Message) = {
    if (!requestHandlerRouter.server.isValid(message.token)) {
      val response = handler.createErrorResponse(
        com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage
      )
      val responseMessage  = message.copy(bodyLength = response.length, body = response)
      sendResponseToClient(responseMessage, ctx)
    }
    else if (isTooBigMessage(message)) {
      logUnsuccessfulProcessing(handler.name, packageTooBigException, message, ctx)
      val response = handler.createErrorResponse(
        packageTooBigException.getMessage
      )
      val responseMessage  = message.copy(bodyLength = response.length, body = response)
      sendResponseToClient(responseMessage, ctx)
    }
    else {
      val response = handler.handleAndGetResponse(message.body)
      val responseMessage  = message.copy(bodyLength = response.length, body = response)

      logSuccessfulProcession(handler.name, message, ctx)
      sendResponseToClient(responseMessage, ctx)
    }
  }

  private def processFutureRequest(handler: RequestHandler,
                                   isTooBigMessage: Message => Boolean,
                                   ctx: ChannelHandlerContext,
                                   f: => ScalaFuture[Unit]
                                  )(message: Message) = {
    if (!requestHandlerRouter.server.isValid(message.token)) {
      val response = handler.createErrorResponse(
        com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidExceptionMessage
      )
      val responseMessage  = message.copy(bodyLength = response.length, body = response)
      sendResponseToClient(responseMessage, ctx)
    }
    else if (isTooBigMessage(message)) {
      logUnsuccessfulProcessing(handler.name, packageTooBigException, message, ctx)
      val response = handler.createErrorResponse(
        packageTooBigException.getMessage
      )
      val responseMessage  = message.copy(bodyLength = response.length, body = response)
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


      if (!requestHandlerRouter.server.isValid(message.token)) {
        logUnsuccessfulProcessing(handler.name, new TokenInvalidException(), message, ctx)
      }
      else if (isTooBigMessage(message)) {
        logUnsuccessfulProcessing(handler.name, packageTooBigException, message, ctx)
      }
      else
        f


  private def processRequestAsyncFireAndForget(context: ExecutionContext,
                                               handler: RequestHandler,
                                               isTooBigMessage: Message => Boolean,
                                               ctx: ChannelHandlerContext
                                              )(message: Message) =
    ScalaFuture {
      if (!requestHandlerRouter.server.isValid(message.token)) {
        logUnsuccessfulProcessing(handler.name, new TokenInvalidException(), message, ctx)
      }
      else if (isTooBigMessage(message)) {
        logUnsuccessfulProcessing(handler.name, packageTooBigException, message, ctx)
      }
      else
        handler.handle(message.body)
    }(context)


  private val orderedExecutionPool = requestHandlerRouter.orderedExecutionPool
  private val subscriberNotifier = requestHandlerRouter.openedTransactionNotifier
  private def processRequestAndReplyClient(handler: RequestHandler,
                                           message: Message,
                                           ctx: ChannelHandlerContext
                                          ): Unit = {
    message.methodId match {
      case Protocol.GetCommitLogOffsets.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutStream.methodID =>
        processRequestAsync(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.CheckStreamExists.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.GetStream.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.DelStream.methodID =>
        processRequestAsync(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.GetTransactionID.methodID =>
        processRequest(handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.GetTransactionIDByTimestamp.methodID =>
        processRequest(handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutTransaction.methodID =>
        processRequestAsync(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutTransactions.methodID =>
        processRequestAsync(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutProducerStateWithData.methodID =>
        processRequestAsync(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.OpenTransaction.methodID =>
        processFutureRequest(handler, isTooBigMetadataMessage, ctx, {
          val args = Protocol.OpenTransaction.decodeRequest(message.body)
          val context = orderedExecutionPool.pool(args.streamID, args.partition)
          ScalaFuture {
            val transactionID =
              requestHandlerRouter.server.getTransactionID

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

            val binaryTransaction = Protocol.PutTransaction.encodeRequest(
              TransactionService.PutTransaction.Args(txn)
            )

            requestHandlerRouter.scheduledCommitLog.putData(
              RecordType.PutTransactionType.id.toByte,
              binaryTransaction
            )

            val response = Protocol.OpenTransaction.encodeResponse(
              TransactionService.OpenTransaction.Result(
                Some(transactionID)
              )
            )

            val responseMessage = message.copy(bodyLength = response.length, body = response)

            logSuccessfulProcession(handler.name, message, ctx)
            sendResponseToClient(responseMessage, ctx)

            subscriberNotifier.notifySubscribers(
              args.streamID,
              args.partition,
              transactionID,
              count = 0,
              TransactionState.Status.Opened,
              args.transactionTTLMs,
              requestHandlerRouter.authOptions.key,
              isNotReliable = false
            )
          }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(handler.name, error, message, ctx)
              val response = handler.createErrorResponse(error.getMessage)
              val responseMessage = message.copy(bodyLength = response.length, body = response)
              sendResponseToClient(responseMessage, ctx)
            }(context)
        })(message)

      case Protocol.PutSimpleTransactionAndData.methodID =>
        processFutureRequest(handler, isTooBigMetadataMessage, ctx, {
          val txn = Protocol.PutSimpleTransactionAndData.decodeRequest(message.body)
          val context = orderedExecutionPool.pool(txn.streamID, txn.partition)
          ScalaFuture {
            val transactionID = requestHandlerRouter.server
              .getTransactionID

            requestHandlerRouter.server.putTransactionData(
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
              Protocol.PutTransactions.encodeRequest(
                TransactionService.PutTransactions.Args(transactions)
              )


            requestHandlerRouter.scheduledCommitLog.putData(
              RecordType.PutTransactionsType.id.toByte,
              messageForPutTransactions
            )

            val response = Protocol.PutSimpleTransactionAndData.encodeResponse(
              TransactionService.PutSimpleTransactionAndData.Result(
                Some(transactionID)
              )
            )

            val responseMessage  = message.copy(bodyLength = response.length, body = response)

            logSuccessfulProcession(handler.name, message, ctx)
            sendResponseToClient(responseMessage, ctx)

            subscriberNotifier.notifySubscribers(
              txn.streamID,
              txn.partition,
              transactionID,
              txn.data.size,
              TransactionState.Status.Instant,
              Long.MaxValue,
              requestHandlerRouter.authOptions.key,
              isNotReliable = false
            )

          }(context)
            .recover { case error =>
              logUnsuccessfulProcessing(handler.name, error, message, ctx)
              val response = handler.createErrorResponse(error.getMessage)
              val responseMessage = message.copy(bodyLength = response.length, body = response)
              sendResponseToClient(responseMessage, ctx)
            }(context)
        })(message)

      case Protocol.GetTransaction.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.GetLastCheckpointedTransaction.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.ScanTransactions.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutTransactionData.methodID =>
        processRequestAsync(serverWriteContext, handler, isTooBigDataMessage, ctx)(message)

      case Protocol.GetTransactionData.methodID =>
        processRequestAsync(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutConsumerCheckpoint.methodID =>
        processRequestAsync(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.GetConsumerState.methodID =>
        processRequestAsync(serverReadContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.Authenticate.methodID =>
        val response = handler.handleAndGetResponse(message.body)
        val responseMessage = message.copy(bodyLength = response.length, body = response)
        logSuccessfulProcession(handler.name, message, ctx)
        sendResponseToClient(responseMessage, ctx)

      case Protocol.IsValid.methodID =>
        val response = handler.handleAndGetResponse(message.body)
        val responseMessage = message.copy(bodyLength = response.length, body = response)
        logSuccessfulProcession(handler.name, message, ctx)
        sendResponseToClient(responseMessage, ctx)

      case Protocol.GetMaxPackagesSizes.methodID =>
        val response = handler.handleAndGetResponse(message.body)
        val responseMessage = message.copy(bodyLength = response.length, body = response)
        logSuccessfulProcession(handler.name, message, ctx)
        sendResponseToClient(responseMessage, ctx)

      case Protocol.GetZKCheckpointGroupServerPrefix.methodID =>
        val response = handler.handleAndGetResponse(message.body)
        val responseMessage = message.copy(bodyLength = response.length, body = response)
        logSuccessfulProcession(handler.name, message, ctx)
        sendResponseToClient(responseMessage, ctx)
    }
  }

  private def processRequestFireAndForgetManner(handler: RequestHandler,
                                                message: Message,
                                                ctx: ChannelHandlerContext
                                               ): Unit =
  {
    message.methodId match {
      case Protocol.PutStream.methodID =>
        processRequestAsyncFireAndForget(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.DelStream.methodID =>
        processRequestAsyncFireAndForget(serverWriteContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutTransaction.methodID =>
        processRequestAsyncFireAndForget(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutTransactions.methodID =>
        processRequestAsyncFireAndForget(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutProducerStateWithData.methodID =>
        processRequestAsyncFireAndForget(commitLogContext, handler, isTooBigMetadataMessage, ctx)(message)

      case Protocol.PutSimpleTransactionAndData.methodID =>
        processFutureRequestFireAndForget(handler, isTooBigDataMessage, ctx, {
          val txn = Protocol.PutSimpleTransactionAndData.decodeRequest(message.body)
          val context = orderedExecutionPool.pool(txn.streamID, txn.partition)
          ScalaFuture {

            val transactionID =
              requestHandlerRouter.server.getTransactionID

            requestHandlerRouter.server.putTransactionData(
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
              Protocol.PutTransactions.encodeRequest(
                TransactionService.PutTransactions.Args(transactions)
              )

            requestHandlerRouter.scheduledCommitLog.putData(
              RecordType.PutTransactionsType.id.toByte,
              messageForPutTransactions
            )

            logSuccessfulProcession(handler.name, message, ctx)

            subscriberNotifier.notifySubscribers(
              txn.streamID,
              txn.partition,
              transactionID,
              txn.data.size,
              TransactionState.Status.Instant,
              Long.MaxValue,
              requestHandlerRouter.authOptions.key,
              isNotReliable = true
            )

          }(context)
        })(message)

      case Protocol.PutTransactionData.methodID =>
        processRequestAsyncFireAndForget(serverWriteContext, handler, isTooBigDataMessage, ctx)(message)

      case Protocol.PutConsumerCheckpoint.methodID =>
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

    val handler = requestHandlerRouter.handler(message.methodId)
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