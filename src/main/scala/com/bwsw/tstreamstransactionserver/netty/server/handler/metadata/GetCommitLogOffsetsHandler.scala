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
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{CommitLogInfo, ServerException, TransactionService}
import GetCommitLogOffsetsHandler.descriptor

private object GetCommitLogOffsetsHandler {
  val descriptor = Protocol.GetCommitLogOffsets
}

class GetCommitLogOffsetsHandler(server: TransactionServer,
                                 scheduledCommitLog: ScheduledCommitLog
                                )
  extends RequestHandler {


  private def process(requestBody: Array[Byte]) = {
    TransactionService.GetCommitLogOffsets.Result(
      Some(CommitLogInfo(
        server.getLastProcessedCommitLogFileID,
        scheduledCommitLog.currentCommitLogFile)
      )
    )
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val response = process(requestBody)
    descriptor.encodeResponse(response)
  }

  override def handle(requestBody: Array[Byte]): Unit = {
//    throw new UnsupportedOperationException(
//      "It doesn't make any sense to get commit log offsets according to fire and forget policy"
//    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetCommitLogOffsets.Result(
        None,
        Some(ServerException(message))
      )
    )
  }

  override def name: String = descriptor.name

  override def id: Byte = descriptor.methodID
}