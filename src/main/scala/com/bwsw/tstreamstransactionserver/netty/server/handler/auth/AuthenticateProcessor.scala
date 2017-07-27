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
package com.bwsw.tstreamstransactionserver.netty.server.handler.auth

import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestProcessor
import com.bwsw.tstreamstransactionserver.rpc.TransactionService
import AuthenticateProcessor.descriptor
import io.netty.channel.ChannelHandlerContext


private object AuthenticateProcessor {
  val descriptor = Protocol.Authenticate
}

class AuthenticateProcessor(server: TransactionServer)
  extends RequestProcessor{

  override val name: String = descriptor.name

  override val id: Byte = descriptor.methodID

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    val authInfo = server.authenticate(args.authKey)
    descriptor.encodeResponse(
      TransactionService.Authenticate.Result(Some(authInfo))
    )
  }

  override protected def handleAndGetResponse(message: Message,
                                              ctx: ChannelHandlerContext): Unit = {
    val updatedMessage = scala.util.Try(process(message.body)) match {
      case scala.util.Success(authInfo) =>
        message.copy(
          bodyLength = authInfo.length,
          body = authInfo
        )
      case scala.util.Failure(throwable) =>
        val response = createErrorResponse(throwable.getMessage)
        message.copy(
          bodyLength = response.length,
          body = response
        )
    }
    sendResponseToClient(updatedMessage, ctx)
  }

  override protected def handle(message: Message,
                                ctx: ChannelHandlerContext): Unit = {
//    throw new UnsupportedOperationException(
//      "It doesn't make any sense to authenticate to fire and forget policy"
//    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException(
      "Authenticate method doesn't imply error at all!"
    )
  }
}
