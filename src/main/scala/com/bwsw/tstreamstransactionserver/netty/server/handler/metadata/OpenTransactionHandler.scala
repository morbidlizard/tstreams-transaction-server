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

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.{RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToRocksWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata.OpenTransactionHandler._
import com.bwsw.tstreamstransactionserver.rpc._


class OpenTransactionHandler(server: TransactionServer,
                             scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  private def process(requestBody: Array[Byte]) = {
    val transactionID = server.getTransactionID
    val args = descriptor.decodeRequest(requestBody)

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

    scheduledCommitLog.putData(
      RecordType.PutTransactionType.id.toByte,
      binaryTransaction
    )

    transactionID
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val transactionID = process(requestBody)
    descriptor.encodeResponse(
      TransactionService.OpenTransaction.Result(Some(transactionID))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    process(requestBody)
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

  override def getName: String = descriptor.name
}

private object OpenTransactionHandler {
  val descriptor = Protocol.OpenTransaction
}

