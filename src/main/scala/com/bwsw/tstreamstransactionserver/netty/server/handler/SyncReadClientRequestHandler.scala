package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import io.netty.channel.ChannelHandlerContext


abstract class SyncReadClientRequestHandler(override final val id: Byte,
                                            override final val name: String)
  extends ClientRequestHandler(id, name) {


  override final def handle(message: RequestMessage,
                            ctx: ChannelHandlerContext,
                            acc: Option[Throwable]): Unit = {
    if (!message.isFireAndForgetMethod) {
      val response = responseImplementation(message, ctx, acc)
      sendResponseToClient(message, response, ctx)
    }
  }

  protected def responseImplementation(message: RequestMessage,
                                       ctx: ChannelHandlerContext,
                                       error: Option[Throwable]): Array[Byte]
}
