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

import com.bwsw.tstreamstransactionserver.netty.Message
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}


trait RequestProcessor
  extends Ordered[RequestProcessor] {

  val id: Byte

  val name: String

  private final val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  protected final def logSuccessfulProcession(method: String,
                                      message: Message,
                                      ctx: ChannelHandlerContext): Unit =
    if (logger.isDebugEnabled)
      logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}: " +
        s"$method is successfully processed!")

  protected final def logUnsuccessfulProcessing(method: String,
                                        error: Throwable,
                                        message: Message,
                                        ctx: ChannelHandlerContext): Unit =
    if (logger.isDebugEnabled)
      logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}: " +
        s"$method is failed while processing!", error)

  protected final def sendResponseToClient(message: Message,
                                     ctx: ChannelHandlerContext): Unit = {
    val binaryResponse = message.toByteArray
    if (ctx.channel().isActive)
      ctx.writeAndFlush(binaryResponse)
  }

  protected def handle(message: Message,
                       ctx: ChannelHandlerContext): Unit

  protected def handleAndGetResponse(message: Message,
                                     ctx: ChannelHandlerContext): Unit

  override final def compare(that: RequestProcessor): Int = {
    java.lang.Byte.compare(this.id, that.id)
  }

  def createErrorResponse(message: String): Array[Byte]

  def process(message: Message, ctx: ChannelHandlerContext): Unit = {
    if (message.isFireAndForgetMethod) {
      handle(message, ctx)
    }
    else {
      handleAndGetResponse(message, ctx)
    }
  }
}