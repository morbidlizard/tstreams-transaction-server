package com.bwsw.netty

import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder


class MessageDecoder extends ByteToMessageDecoder {
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val readableBytes = in.readableBytes()
    if (readableBytes < Message.headerSize){}
    else {
      in.markReaderIndex()
      val length = in.readInt()
      val protocol = in.readByte()
      if (length <= (readableBytes - Message.headerSize)) {
        val message = new Array[Byte](length)
        val buffer = in.readSlice(length)
        buffer.readBytes(message)
        out.add(Message(length, protocol,message))
      } else in.resetReaderIndex()
    }
  }
}
