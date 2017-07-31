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
package com.bwsw.tstreamstransactionserver.netty.server


import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.exception.Throwable.{PackageTooBigException, TokenInvalidException}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.CommitLogToRocksWriter
import com.bwsw.tstreamstransactionserver.netty.server.handler.{RequestProcessor, RequestProcessorRouter}
import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionService, TransactionStates}
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}


class ServerHandler(requestHandlerRouter: RequestProcessorRouter,
                    executionContext:ServerExecutionContextGrids,
                    logger: Logger)
  extends SimpleChannelInboundHandler[ByteBuf]
{
  override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
    val message = Message.fromByteBuf(buf)
    invokeMethod(message, ctx)
  }

  protected def invokeMethod(message: Message, ctx: ChannelHandlerContext): Unit = {

    if (logger.isDebugEnabled)
      logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id} method is invoked.")

    requestHandlerRouter.process(message, ctx, None)

  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.channel().close()
  }
}