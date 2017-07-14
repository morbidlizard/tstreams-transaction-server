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
package com.bwsw.tstreamstransactionserver.netty.server.handler.data

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc._
import PutSimpleTransactionAndDataHandler.descriptor

private object PutSimpleTransactionAndDataHandler {
  val descriptor = Protocol.PutSimpleTransactionAndData
}

class PutSimpleTransactionAndDataHandler(server: TransactionServer,
                                         scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  private def process(requestBody: Array[Byte]) = {
    val transactionID = server.getTransactionID
    val txn = descriptor.decodeRequest(requestBody)
    server.putTransactionData(
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
          txn.data.size, 3L
        )), None
      ),
      Transaction(Some(
        ProducerTransaction(
          txn.streamID,
          txn.partition,
          transactionID,
          TransactionStates.Checkpointed,
          txn.data.size,
          120L)), None
      )
    )
    val messageForPutTransactions = Protocol.PutTransactions.encodeRequest(
      TransactionService.PutTransactions.Args(transactions)
    )

    scheduledCommitLog.putData(
      CommitLogToBerkeleyWriter.putTransactionsType,
      messageForPutTransactions
    )
    transactionID
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val transactionID = process(requestBody)
//    logSuccessfulProcession(Descriptors.PutSimpleTransactionAndData.name)
    Protocol.PutSimpleTransactionAndData.encodeResponse(
      TransactionService.PutSimpleTransactionAndData.Result(
        Some(transactionID)
      )
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    process(requestBody)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutSimpleTransactionAndData.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override def name: String = descriptor.name

  override def id: Byte = descriptor.methodID
}
