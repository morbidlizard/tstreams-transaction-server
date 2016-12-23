package netty

import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder


class MessageDecoder extends ByteToMessageDecoder {
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val readableBytes = in.readableBytes()
    if (readableBytes < Message.headerSize){}
    else {
      val length = in.readInt()
      val descriptor = in.readByte()
      if (length <= (readableBytes - Message.headerSize)) {
        val message = new Array[Byte](length)
        in.readBytes(message)
        out.add(Message(length, Descriptor(descriptor), message))
      }// else in.resetReaderIndex()
    }
  }
}
