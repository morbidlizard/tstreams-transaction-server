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
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateProcessor, IsValidProcessor}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.{GetConsumerStateProcessor, PutConsumerCheckpointProcessor}
import com.bwsw.tstreamstransactionserver.netty.server.handler.data._
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsProcessor, DelStreamProcessor, GetStreamProcessor, PutStreamProcessor}
import com.bwsw.tstreamstransactionserver.netty.server.handler.test._
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.{GetMaxPackagesSizesProcessor, GetZKCheckpointGroupServerPrefixProcessor}
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenTransactionStateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportService
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthenticationOptions, ServerRoleOptions, TransportOptions}

import scala.collection.Searching._
import scala.concurrent.ExecutionContext
import RequestProcessorRouter._
import com.bwsw.tstreamstransactionserver.netty.Message
import io.netty.channel.ChannelHandlerContext

private object RequestProcessorRouter {

  final def handlerId(clientRequestHandler: ClientRequestHandler): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> clientRequestHandler
  }

  final def handlerAuthData(clientRequestHandler: ClientRequestHandler)
                           (implicit
                            authService: AuthService,
                            transportService: TransportService): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthValidatorHandler(
      new DataPackageSizeValidatorHandler(
        clientRequestHandler,
        transportService
      ),
      authService
    )
  }

  final def handlerAuthMetadata(clientRequestHandler: ClientRequestHandler)
                               (implicit
                                authService: AuthService,
                                transportService: TransportService): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthValidatorHandler(
      new MetadataPackageSizeValidatorHandler(
        clientRequestHandler,
        transportService
      ),
      authService
    )
  }

  final def handlerAuth(clientRequestHandler: ClientRequestHandler)
                       (implicit
                        authService: AuthService): (Byte, RequestHandler) = {
    val id = clientRequestHandler.id
    id -> new AuthValidatorHandler(
      clientRequestHandler,
      authService
    )
  }

}

final class RequestProcessorRouter(server: TransactionServer,
                                   scheduledCommitLog: ScheduledCommitLog,
                                   packageTransmissionOpts: TransportOptions,
                                   authOptions: AuthenticationOptions,
                                   orderedExecutionPool: OrderedExecutionContextPool,
                                   notifier: OpenTransactionStateNotifier,
                                   serverRoleOptions: ServerRoleOptions,
                                   executionContext:ServerExecutionContextGrids)
  extends RequestHandler {
  private implicit val authService =
    new AuthService(authOptions)

  private implicit val transportService =
    new TransportService(packageTransmissionOpts)

  private val serverWriteContext: ExecutionContext =
    executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext =
    executionContext.serverReadContext
  private val commitLogContext =
    executionContext.commitLogContext

  private val (handlersIDs, handlers) = Array(

    handlerAuth(new GetCommitLogOffsetsProcessor(
      server,
      scheduledCommitLog,
      serverReadContext
    )),

    handlerAuth(new PutStreamProcessor(
      server,
      serverReadContext
    )),


    handlerAuth(new CheckStreamExistsProcessor(
      server,
      serverReadContext
    )),

    handlerAuth(new GetStreamProcessor(
      server,
      serverReadContext
    )),
    handlerAuth(new DelStreamProcessor(
      server,
      serverWriteContext
    )),


    handlerAuth(new GetTransactionIDProcessor(
      server
    )),
    handlerAuth(new GetTransactionIDByTimestampProcessor(
      server
    )),


    handlerAuthMetadata(new PutTransactionProcessor(
      server,
      scheduledCommitLog,
      commitLogContext
    )),
    handlerAuthMetadata(new PutTransactionsProcessor(
      server,
      scheduledCommitLog,
      commitLogContext
    )),
    handlerAuthData(new OpenTransactionProcessor(
      server,
      scheduledCommitLog,
      notifier,
      authOptions,
      orderedExecutionPool
    )),
    handlerAuth(new GetTransactionProcessor(
      server,
      serverReadContext
    )),
    handlerAuth(new GetLastCheckpointedTransactionProcessor(
      server,
      serverReadContext
    )),
    handlerAuth(new ScanTransactionsProcessor(
      server,
      serverReadContext
    )),


    handlerAuthData(new PutProducerStateWithDataProcessor(
      server,
      scheduledCommitLog,
      commitLogContext
    )),
    handlerAuthData(new PutSimpleTransactionAndDataProcessor(
      server,
      scheduledCommitLog,
      notifier,
      authOptions,
      orderedExecutionPool
    )),
    handlerAuthData(new PutTransactionDataProcessor(
      server,
      serverWriteContext
    )),
    handlerAuth(new GetTransactionDataProcessor(
      server,
      serverReadContext
    )),


    handlerAuthMetadata(new PutConsumerCheckpointProcessor(
      server,
      scheduledCommitLog,
      commitLogContext
    )),
    handlerAuth(new GetConsumerStateProcessor(
      server,
      serverReadContext
    )),


    handlerId(new AuthenticateProcessor(
      authService
    )),
    handlerId(new IsValidProcessor(
      authService
    )),


    handlerId(new GetMaxPackagesSizesProcessor(
      packageTransmissionOpts
    )),
    handlerId(new GetZKCheckpointGroupServerPrefixProcessor(
      serverRoleOptions
    ))
  ).sortBy(_._1).unzip


  override def process(message: Message,
                       ctx: ChannelHandlerContext,
                       acc: Option[Throwable]): Unit = {
    handlersIDs.search(message.methodId) match {
      case Found(index) =>
        val handler = handlers(index)
        handler.process(message, ctx, None)
      case _ =>
        throw new IllegalArgumentException(s"Not implemented method that has id: ${message.methodId}")
    }
  }
}
