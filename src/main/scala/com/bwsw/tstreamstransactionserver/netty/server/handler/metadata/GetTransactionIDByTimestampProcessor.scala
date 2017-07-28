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
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestWithValidationProcessor
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}
import GetTransactionIDByTimestampProcessor.descriptor
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportService
import io.netty.channel.ChannelHandlerContext


private object GetTransactionIDByTimestampProcessor {
  val descriptor = Protocol.GetTransactionIDByTimestamp
}

class GetTransactionIDByTimestampProcessor(server: TransactionServer,
                                           authService: AuthService,
                                           transportService: TransportService)
  extends RequestWithValidationProcessor(
    authService,
    transportService) {
  override val name: String = descriptor.name

  override val id: Byte = descriptor.methodID

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    server.getTransactionIDByTimestamp(args.timestamp)
  }

  override protected def handle(message: Message,
                                ctx: ChannelHandlerContext): Unit = {
//    throw new UnsupportedOperationException(
//      "It doesn't make any sense to get transaction ID by timestamp according to fire and forget policy"
//    )
  }

  override protected def handleAndGetResponse(message: Message,
                                              ctx: ChannelHandlerContext): Unit = {
    val exceptionOpt = validate(message, ctx)
    if (exceptionOpt.isEmpty) {
      val response = descriptor.encodeResponse(
        TransactionService.GetTransactionIDByTimestamp.Result(
          Some(process(message.body))
        )
      )
      val responseMessage = message.copy(
        bodyLength = response.length,
        body = response
      )
      sendResponseToClient(responseMessage, ctx)
    } else {
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
      TransactionService.GetTransactionIDByTimestamp.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}
