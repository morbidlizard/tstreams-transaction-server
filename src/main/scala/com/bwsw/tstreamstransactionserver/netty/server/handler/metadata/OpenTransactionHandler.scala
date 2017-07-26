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
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc._
import OpenTransactionHandler.descriptor
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.OpenTransaction

import scala.concurrent.Future

private object OpenTransactionHandler {
  val descriptor = Protocol.OpenTransaction
}

class OpenTransactionHandler(server: TransactionServer,
                             scheduledCommitLog: ScheduledCommitLog,
                             orderedExecutionPool: OrderedExecutionContextPool)
  extends RequestHandler {

  private def process(transactionId: Long,
                      args: OpenTransaction.Args): Long = {
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

    transactionId
  }

  private def processFuture[T](requestBody: Array[Byte],
                               idToEntity: Long => T) = {
    val transactionID = server.getTransactionID
    val args = descriptor.decodeRequest(requestBody)
    val context = orderedExecutionPool.pool(args.streamID, args.partition)
    Future {
      idToEntity(
        process(transactionID, args))
    }(context)
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Future[Array[Byte]] = {
    processFuture[Array[Byte]](
      requestBody,
      transactionID => {
        descriptor.encodeResponse(
          TransactionService.OpenTransaction.Result(Some(transactionID))
        )
      }
    )
  }

  override def handle(requestBody: Array[Byte]): Future[Unit]= {
    processFuture(
      requestBody,
      _ => ()
    )
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

  override def name: String = descriptor.name

  override def id: Byte = descriptor.methodID
}
