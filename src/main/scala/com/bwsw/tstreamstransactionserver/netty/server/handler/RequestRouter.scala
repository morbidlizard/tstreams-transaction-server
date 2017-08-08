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
package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter._
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, IsValidHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.{GetConsumerStateHandler, PutConsumerCheckpointHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.data._
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsHandler, DelStreamHandler, GetStreamHandler, PutStreamHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.{GetMaxPackagesSizesHandler, GetZKCheckpointGroupServerPrefixHandler}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthenticationOptions, CheckpointGroupRoleOptions, TransportOptions}
import io.netty.channel.ChannelHandlerContext

import scala.collection.Searching._
import scala.concurrent.ExecutionContext

private object RequestRouter {

  final def handlerId(clientRequestHandler: ClientRequestHandler): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> clientRequestHandler
  }

  final def handlerAuthData(clientRequestHandler: ClientRequestHandler)
                           (implicit
                            authService: AuthService,
                            transportValidator: TransportValidator): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthHandler(
      new DataPackageSizeValidationHandler(
        clientRequestHandler,
        transportValidator
      ),
      authService
    )
  }

  final def handlerAuthMetadata(clientRequestHandler: ClientRequestHandler)
                               (implicit
                                authService: AuthService,
                                transportValidator: TransportValidator): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthHandler(
      new MetadataPackageSizeValidationHandler(
        clientRequestHandler,
        transportValidator
      ),
      authService
    )
  }

  final def handlerAuth(clientRequestHandler: ClientRequestHandler)
                       (implicit
                        authService: AuthService): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthHandler(
      clientRequestHandler,
      authService
    )
  }

}

final class RequestRouter(server: TransactionServer,
                          oneNodeCommitLogService: CommitLogService,
                          scheduledCommitLog: ScheduledCommitLog,
                          packageTransmissionOpts: TransportOptions,
                          authOptions: AuthenticationOptions,
                          orderedExecutionPool: OrderedExecutionContextPool,
                          notifier: OpenedTransactionNotifier,
                          serverRoleOptions: CheckpointGroupRoleOptions,
                          executionContext: ServerExecutionContextGrids) {
  private implicit val authService =
    new AuthService(authOptions)

  private implicit val transportValidator =
    new TransportValidator(packageTransmissionOpts)

  private val serverWriteContext: ExecutionContext =
    executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext =
    executionContext.serverReadContext
  private val commitLogContext =
    executionContext.commitLogContext

  private val (handlersIDs: Array[Byte], handlers: Array[RequestHandler]) = Array(

    handlerAuth(new GetCommitLogOffsetsHandler(
      oneNodeCommitLogService,
      scheduledCommitLog,
      serverReadContext
    )),

    handlerAuth(new PutStreamHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new CheckStreamExistsHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new GetStreamHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new DelStreamHandler(
      server,
      serverWriteContext
    )),

    handlerAuth(new GetTransactionIDHandler(
      server
    )),
    handlerAuth(new GetTransactionIDByTimestampHandler(
      server
    )),

    handlerAuthMetadata(new PutTransactionHandler(
      server,
      scheduledCommitLog,
      commitLogContext
    )),

    handlerAuthMetadata(new PutTransactionsHandler(
      server,
      scheduledCommitLog,
      commitLogContext
    )),

    handlerAuthData(new OpenTransactionHandler(
      server,
      scheduledCommitLog,
      notifier,
      authOptions,
      orderedExecutionPool
    )),

    handlerAuth(new GetTransactionHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new GetLastCheckpointedTransactionHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new ScanTransactionsHandler(
      server,
      serverReadContext
    )),

    handlerAuthData(new PutProducerStateWithDataHandler(
      server,
      scheduledCommitLog,
      commitLogContext
    )),

    handlerAuthData(new PutSimpleTransactionAndDataHandler(
      server,
      scheduledCommitLog,
      notifier,
      authOptions,
      orderedExecutionPool
    )),

    handlerAuthData(new PutTransactionDataHandler(
      server,
      serverWriteContext
    )),

    handlerAuth(new GetTransactionDataHandler(
      server,
      serverReadContext
    )),

    handlerAuthMetadata(new PutConsumerCheckpointHandler(
      server,
      scheduledCommitLog,
      commitLogContext
    )),
    handlerAuth(new GetConsumerStateHandler(
      server,
      serverReadContext
    )),

    handlerId(new AuthenticateHandler(
      authService
    )),
    handlerId(new IsValidHandler(
      authService
    )),

    handlerId(new GetMaxPackagesSizesHandler(
      packageTransmissionOpts
    )),

    handlerId(new GetZKCheckpointGroupServerPrefixHandler(
      serverRoleOptions
    ))
  ).sortBy(_._1).unzip


  def route(message: RequestMessage,
            ctx: ChannelHandlerContext,
            acc: Option[Throwable]): Unit = {
    handlersIDs.search(message.methodId) match {
      case Found(index) =>
        val handler = handlers(index)
        handler.handle(message, ctx, None)
      case _ =>
        throw new IllegalArgumentException(s"Not implemented method that has id: ${message.methodId}")
    }
  }
}
