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

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.TransactionService
import IsValidHandler._

import scala.concurrent.Future

private object IsValidHandler {
  val descriptor = Protocol.IsValid
}

class IsValidHandler(server: TransactionServer)
  extends RequestHandler{

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    val result = server.isValid(args.token)
    descriptor.encodeResponse(
      TransactionService.IsValid.Result(Some(result))
    )
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Future[Array[Byte]] = {
    scala.util.Try(process(requestBody)) match {
      case scala.util.Success(isValid) =>
        Future.successful(isValid)
      case scala.util.Failure(throwable) =>
        Future.failed(throwable)
    }
  }

  override def handle(requestBody: Array[Byte]): Future[Unit] = {
    Future.failed(
      throw new UnsupportedOperationException(
        "It doesn't make any sense to check if token is valid according to fire and forget policy"
      )
    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException("isValid method can't throw error at all!")
  }

  override def name: String = descriptor.name

  override def id: Byte = descriptor.methodID
}
