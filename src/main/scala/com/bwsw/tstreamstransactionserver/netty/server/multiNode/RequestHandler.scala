package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import com.bwsw.tstreamstransactionserver.netty.Message
import io.netty.channel.ChannelHandlerContext

trait RequestHandler {
  def getName: String
  def handleAndSendResponse(requestBody: Array[Byte],
                            message: Message,
                            connection: ChannelHandlerContext): Unit
  def handleFireAndForget(requestBody: Array[Byte]): Unit
  def createErrorResponse(message: String): Array[Byte]
}
