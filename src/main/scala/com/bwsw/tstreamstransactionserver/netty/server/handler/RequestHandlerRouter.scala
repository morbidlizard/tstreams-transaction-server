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

import com.bwsw.tstreamstransactionserver.netty.Protocol._
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.{GetConsumerStateHandler, PutConsumerCheckpointHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.data.{GetTransactionDataHandler, PutProducerStateWithDataHandler, PutTransactionDataHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsHandler, DelStreamHandler, GetStreamHandler, PutStreamHandler}
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenTransactionStateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthenticationOptions, TransportOptions}

final class RequestHandlerRouter(val server: TransactionServer,
                                 val scheduledCommitLog: ScheduledCommitLog,
                                 val packageTransmissionOpts: TransportOptions,
                                 val authOptions: AuthenticationOptions,
                                 val orderedExecutionPool: OrderedExecutionContextPool,
                                 val openTransactionStateNotifier: OpenTransactionStateNotifier
                                 ) {

  private val commitLogOffsetsHandler =
    new GetCommitLogOffsetsHandler(server, scheduledCommitLog)

  private val putStreamHandler =
    new PutStreamHandler(server)
  private val checkStreamExistsHandler =
    new CheckStreamExistsHandler(server)
  private val getStreamHandler =
    new GetStreamHandler(server)
  private val delStreamHandler =
    new DelStreamHandler(server)

  private val getTransactionIDHandler =
    new GetTransactionIDHandler(server)
  private val getTransactionIDByTimestampHandler =
    new GetTransactionIDByTimestampHandler(server)

  private val putTransactionHandler =
    new PutTransactionHandler(server, scheduledCommitLog)
  private val putTransactionsHandler =
    new PutTransactionsHandler(server, scheduledCommitLog)
  private val putProducerStateWithDataHandler =
    new PutProducerStateWithDataHandler(server, scheduledCommitLog)
  private val putSimpleTransactionAndDataHandler =
    new PutSimpleTransactionAndDataHandler(server, scheduledCommitLog)
  private val openTransactionHandler =
    new OpenTransactionHandler(server, scheduledCommitLog)
  private val getTransactionHandler =
    new GetTransactionHandler(server)
  private val getLastCheckpointedTransaction =
    new GetLastCheckpointedTransactionHandler(server)
  private val scanTransactionsHandler =
    new ScanTransactionsHandler(server)
  private val putTransactionDataHandler =
    new PutTransactionDataHandler(server)
  private val getTransactionDataHandler =
    new GetTransactionDataHandler(server)

  private val putConsumerCheckpointHandler =
    new PutConsumerCheckpointHandler(server, scheduledCommitLog)
  private val getConsumerStateHandler =
    new GetConsumerStateHandler(server)

  private val authenticateHandler =
    new AuthenticateHandler(server, packageTransmissionOpts)
  private val isValidHandler =
    new IsValidHandler(server)


  def handler(id: Byte): RequestHandler = id match {
    case GetCommitLogOffsets.methodID =>
      commitLogOffsetsHandler

    case PutStream.methodID =>
      putStreamHandler
    case CheckStreamExists.methodID =>
      checkStreamExistsHandler
    case GetStream.methodID =>
      getStreamHandler
    case DelStream.methodID =>
      delStreamHandler

    case GetTransactionID.methodID =>
      getTransactionIDHandler
    case GetTransactionIDByTimestamp.methodID =>
      getTransactionIDByTimestampHandler

    case PutTransaction.methodID =>
      putTransactionHandler
    case PutTransactions.methodID =>
      putTransactionsHandler
    case PutProducerStateWithData.methodID =>
      putProducerStateWithDataHandler
    case PutSimpleTransactionAndData.methodID =>
      putSimpleTransactionAndDataHandler
    case OpenTransaction.methodID =>
      openTransactionHandler
    case GetTransaction.methodID =>
      getTransactionHandler
    case GetLastCheckpointedTransaction.methodID =>
      getLastCheckpointedTransaction
    case ScanTransactions.methodID =>
      scanTransactionsHandler
    case PutTransactionData.methodID =>
      putTransactionDataHandler
    case GetTransactionData.methodID =>
      getTransactionDataHandler

    case PutConsumerCheckpoint.methodID =>
      putConsumerCheckpointHandler
    case GetConsumerState.methodID =>
      getConsumerStateHandler

    case Authenticate.methodID =>
      authenticateHandler
    case IsValid.methodID =>
      isValidHandler

    case methodID =>
      throw new IllegalArgumentException(s"Not implemented method that has id: $methodID")
  }

}
