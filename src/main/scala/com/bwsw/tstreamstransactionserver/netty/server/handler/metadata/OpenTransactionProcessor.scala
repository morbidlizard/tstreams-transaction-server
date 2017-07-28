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
package com.bwsw.tstreamstransactionserver.netty.server.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestWithValidationProcessor
import com.bwsw.tstreamstransactionserver.rpc._
import OpenTransactionProcessor.descriptor
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenTransactionStateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportService
import com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthenticationOptions
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.OpenTransaction
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.Future

private object OpenTransactionProcessor {
  val descriptor = Protocol.OpenTransaction
}

class OpenTransactionProcessor(server: TransactionServer,
                               scheduledCommitLog: ScheduledCommitLog,
                               notifier: OpenTransactionStateNotifier,
                               authOptions: AuthenticationOptions,
                               orderedExecutionPool: OrderedExecutionContextPool,
                               authService: AuthService,
                               transportService: TransportService)
  extends RequestWithValidationProcessor(
    authService,
    transportService) {

  override val name: String = descriptor.name

  override val id: Byte = descriptor.methodID

  private def process(args: OpenTransaction.Args,
                      transactionId: Long): Unit = {

    val txn = Transaction(Some(
      ProducerTransaction(
        args.streamID,
        args.partition,
        transactionId,
        TransactionStates.Opened,
        quantity = 0,
        ttl = args.transactionTTLMs
      )), None
    )

    val binaryTransaction = Protocol.PutTransaction.encodeRequest(
      TransactionService.PutTransaction.Args(txn)
    )

    scheduledCommitLog.putData(
      RecordType.PutTransactionType.id.toByte,
      binaryTransaction
    )
  }

  override protected def handle(message: Message,
                                ctx: ChannelHandlerContext): Unit = {
    val exceptionOpt = validate(message, ctx)
    if (exceptionOpt.isEmpty) {
      val args = descriptor.decodeRequest(message.body)
      val context = orderedExecutionPool.pool(args.streamID, args.partition)
      Future {
        val transactionID =
          server.getTransactionID

        process(args, transactionID)

        notifier.notifySubscribers(
          args.streamID,
          args.partition,
          transactionID,
          count = 0,
          TransactionState.Status.Opened,
          args.transactionTTLMs,
          authOptions.key,
          isNotReliable = true
        )
      }(context)
    }
  }

  override protected def handleAndGetResponse(message: Message,
                                              ctx: ChannelHandlerContext): Unit = {
    val exceptionOpt = validate(message, ctx)
    if (exceptionOpt.isEmpty) {
      val args = descriptor.decodeRequest(message.body)
      val context = orderedExecutionPool.pool(args.streamID, args.partition)
      Future {
        val transactionID =
          server.getTransactionID

        process(args, transactionID)

        val response = descriptor.encodeResponse(
          TransactionService.OpenTransaction.Result(
            Some(transactionID)
          )
        )

        val responseMessage = message.copy(
          bodyLength = response.length,
          body = response
        )
        sendResponseToClient(responseMessage, ctx)

        notifier.notifySubscribers(
          args.streamID,
          args.partition,
          transactionID,
          count = 0,
          TransactionState.Status.Opened,
          args.transactionTTLMs,
          authOptions.key,
          isNotReliable = false
        )
      }(context)
        .recover { case error =>
          logUnsuccessfulProcessing(name, error, message, ctx)
          val response = createErrorResponse(error.getMessage)
          val responseMessage = message.copy(
            bodyLength = response.length,
            body = response
          )
          sendResponseToClient(responseMessage, ctx)
        }(context)
    }
    else {
      val error = exceptionOpt.get
      logUnsuccessfulProcessing(name, error, message, ctx)
      val response = createErrorResponse(error.getMessage)
      val responseMessage = message.copy(
        bodyLength = response.length,
        body = response
      )
      sendResponseToClient(responseMessage, ctx)
    }
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.OpenTransaction.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}
