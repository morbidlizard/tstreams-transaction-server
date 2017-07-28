package com.bwsw.tstreamstransactionserver.netty.server.handler.test
import com.bwsw.tstreamstransactionserver.netty.Message
import io.netty.channel.ChannelHandlerContext



abstract class ClientFireAndForgetReadHandler(override final val id: Byte,
                                              override final val name: String)
  extends ClientRequestHandler(id, name) {


  protected def fireAndReplyImplementation(message: Message,
                                           ctx: ChannelHandlerContext,
                                           acc: Option[Throwable]): Unit

  override final def process(message: Message,
                       ctx: ChannelHandlerContext,
                       acc: Option[Throwable]): Unit = {
    if (!message.isFireAndForgetMethod)
      fireAndReplyImplementation(message, ctx, acc)
  }
}
