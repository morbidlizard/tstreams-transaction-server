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

import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, IsValidHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.{GetConsumerStateHandler, PutConsumerCheckpointHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.data.{GetTransactionDataHandler, PutProducerStateWithDataHandler, PutSimpleTransactionAndDataHandler, PutTransactionDataHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsHandler, DelStreamHandler, GetStreamHandler, PutStreamHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.GetMaxPackagesSizesHandler
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenTransactionStateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthenticationOptions, ServerRoleOptions, TransportOptions}

import scala.collection.Searching._

final class RequestHandlerRouter(val server: TransactionServer,
                                 val scheduledCommitLog: ScheduledCommitLog,
                                 val packageTransmissionOpts: TransportOptions,
                                 val authOptions: AuthenticationOptions,
                                 val orderedExecutionPool: OrderedExecutionContextPool,
                                 val openTransactionStateNotifier: OpenTransactionStateNotifier,
                                 val serverRoleOptions: ServerRoleOptions) {

  private val handlers: Array[RequestHandler] = Array(
    new GetCommitLogOffsetsHandler(server, scheduledCommitLog),

    new PutStreamHandler(server),
    new CheckStreamExistsHandler(server),
    new GetStreamHandler(server),
    new DelStreamHandler(server),

    new GetTransactionIDHandler(server),
    new GetTransactionIDByTimestampHandler(server),

    new PutTransactionHandler(server, scheduledCommitLog),
    new PutTransactionsHandler(server, scheduledCommitLog),
    new OpenTransactionHandler(server, scheduledCommitLog),
    new GetTransactionHandler(server),
    new GetLastCheckpointedTransactionHandler(server),
    new ScanTransactionsHandler(server),

    new PutProducerStateWithDataHandler(server, scheduledCommitLog),
    new PutSimpleTransactionAndDataHandler(server, scheduledCommitLog),
    new PutTransactionDataHandler(server),
    new GetTransactionDataHandler(server),

    new PutConsumerCheckpointHandler(server, scheduledCommitLog),
    new GetConsumerStateHandler(server),

    new AuthenticateHandler(server, packageTransmissionOpts),
    new IsValidHandler(server),

    new GetMaxPackagesSizesHandler(packageTransmissionOpts),
    new GetZKCheckpointGroupServerPrefixHandler(serverRoleOptions)
  ).sorted


  private val handlersIDs = handlers.map(_.id)
  def handler(id: Byte): RequestHandler =
    handlersIDs.search(id) match {
      case Found(index) => handlers(index)
      case _ =>
        throw new IllegalArgumentException(s"Not implemented method that has id: $id")
    }
}
