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
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateProcessor, IsValidProcessor}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.{GetConsumerStateProcessor, PutConsumerCheckpointProcessor}
import com.bwsw.tstreamstransactionserver.netty.server.handler.data.{GetTransactionDataProcessor, PutProducerStateWithDataProcessor, PutSimpleTransactionAndDataProcessor, PutTransactionDataProcessor}
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsProcessor, DelStreamProcessor, GetStreamProcessor, PutStreamProcessor}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.GetMaxPackagesSizesProcessor
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



  private val handlers: Array[RequestProcessor] = Array(
    new GetCommitLogOffsetsProcessor(server, scheduledCommitLog),

    new PutStreamProcessor(server),
    new CheckStreamExistsProcessor(server),
    new GetStreamProcessor(server),
    new DelStreamProcessor(server),

    new GetTransactionIDProcessor(server),
    new GetTransactionIDByTimestampProcessor(server),

    new PutTransactionProcessor(server, scheduledCommitLog),
    new PutTransactionsProcessor(server, scheduledCommitLog),
    new OpenTransactionProcessor(server, scheduledCommitLog),
    new GetTransactionProcessor(server),
    new GetLastCheckpointedTransactionProcessor(server),
    new ScanTransactionsProcessor(server),

    new PutProducerStateWithDataProcessor(server, scheduledCommitLog),
    new PutSimpleTransactionAndDataProcessor(server, scheduledCommitLog),
    new PutTransactionDataProcessor(server),
    new GetTransactionDataProcessor(server),

    new PutConsumerCheckpointProcessor(server, scheduledCommitLog),
    new GetConsumerStateProcessor(server),

    new AuthenticateProcessor(server, packageTransmissionOpts),
    new IsValidProcessor(server),

    new GetMaxPackagesSizesProcessor(packageTransmissionOpts),
    new GetZKCheckpointGroupServerPrefixProcessor(serverRoleOptions)
  ).sorted


  private val handlersIDs = handlers.map(_.id)
  def handler(id: Byte): RequestProcessor =
    handlersIDs.search(id) match {
      case Found(index) => handlers(index)
      case _ =>
        throw new IllegalArgumentException(s"Not implemented method that has id: $id")
    }
}
