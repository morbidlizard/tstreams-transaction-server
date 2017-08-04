package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter
import io.netty.channel.ChannelHandlerContext

class CheckpointGroupHandlerRouter
  extends RequestRouter{

  override def route(message: RequestMessage,
                     ctx: ChannelHandlerContext): Unit = {

  }
}
