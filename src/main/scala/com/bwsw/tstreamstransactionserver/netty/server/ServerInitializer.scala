package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.{Message, MessageDecoder}
import io.netty.channel.{ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder



class ServerInitializer(serverHandler: => SimpleChannelInboundHandler[Message]) extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(new ByteArrayEncoder())
      .addLast(new MessageDecoder)
      .addLast(serverHandler)
  }
}