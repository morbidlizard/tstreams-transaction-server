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

import com.bwsw.tstreamstransactionserver.netty.{RequestMessage, Protocol}
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.rpc.{CommitLogInfo, ServerException, TransactionService}
import GetCommitLogOffsetsProcessor.descriptor
import com.bwsw.tstreamstransactionserver.netty.server.handler.AsyncClientRequestHandler
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.ExecutionContext

private object GetCommitLogOffsetsProcessor {
  val descriptor = Protocol.GetCommitLogOffsets
}

class GetCommitLogOffsetsProcessor(server: TransactionServer,
                                   scheduledCommitLog: ScheduledCommitLog,
                                   context: ExecutionContext)
  extends AsyncClientRequestHandler(
    descriptor.methodID,
    descriptor.name,
    context) {

  private def process(requestBody: Array[Byte]) = {
    CommitLogInfo(
      server.getLastProcessedCommitLogFileID,
      scheduledCommitLog.currentCommitLogFile
    )
  }

  override protected def fireAndForgetImplementation(message: RequestMessage): Unit = {}

  override protected def fireAndReplyImplementation(message: RequestMessage,
                                                    ctx: ChannelHandlerContext): Array[Byte] = {
    val response = descriptor.encodeResponse(
      TransactionService.GetCommitLogOffsets.Result(
        Some(process(message.body))
      )
    )
    response
  }


  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetCommitLogOffsets.Result(
        None,
        Some(ServerException(message))
      )
    )
  }
}