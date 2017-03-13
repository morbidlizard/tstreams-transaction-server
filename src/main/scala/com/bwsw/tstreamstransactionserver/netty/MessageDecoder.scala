package com.bwsw.tstreamstransactionserver.netty

import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder

/** Message Decoder is aggregator for all bytes incoming via tcp session and deserialize these bytes to a new Message if
  * there are enough bytes for doing it.
  *
  *  @constructor create a message decoder handler.
  *
  */
class MessageDecoder extends ByteToMessageDecoder {


  /** Deserialize bytes to a message and puts it to a list for next processing for other handlers. */
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val readableBytes = in.readableBytes()
    if (readableBytes < Message.headerSize){}
    else {
      in.markReaderIndex()
      val length = in.readInt()
      val protocol = in.readByte()
      val token = in.readInt()
      if (length <= (readableBytes - Message.headerSize)) {
        val message = new Array[Byte](length)
        val buffer = in.readSlice(length)
        buffer.readBytes(message)
        out.add(Message(length, protocol, message, token))
      } else in.resetReaderIndex()
    }
  }
}
