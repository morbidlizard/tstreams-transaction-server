package com.bwsw.tstreamstransactionserver.netty.server.handler.test

import com.bwsw.tstreamstransactionserver.netty.Message
import io.netty.channel.ChannelHandlerContext

trait RequestHandler {
  def process(message: Message,
              ctx: ChannelHandlerContext,
              acc: Option[Throwable]): Unit
}
