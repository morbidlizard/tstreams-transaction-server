package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import io.netty.buffer.ByteBuf
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.ByteArrayEncoder



class ServerInitializer(serverHandler: => SimpleChannelInboundHandler[ByteBuf], packageTransmissionOpts: TransportOptions) extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(new ByteArrayEncoder())
      .addLast(new LengthFieldBasedFrameDecoder(
        Int.MaxValue,
        //packageTransmissionOpts.maxDataPackageSize max packageTransmissionOpts.maxMetadataPackageSize,
        Message.headerFieldSize,
        Message.lengthFieldSize)
      )
      .addLast(serverHandler)
  }
}