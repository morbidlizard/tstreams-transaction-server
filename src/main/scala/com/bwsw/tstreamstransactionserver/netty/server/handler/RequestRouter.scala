package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import io.netty.channel.ChannelHandlerContext

trait RequestRouter {
  def route(message: RequestMessage,
            ctx: ChannelHandlerContext): Unit
}
