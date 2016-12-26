package netty.client

import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import netty.{Descriptors, Message}

class ClientHandler extends SimpleChannelInboundHandler[Message] {
  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    println(Descriptors.PutStream.decodeResponse(msg))
  }


//  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
//    val buffer = msg.asInstanceOf[ByteBuf]
//    try {
//      val response = buffer.readBoolean()
//      println(response)
//      ctx.close()
//    }
//    finally buffer.release()
//  }
  
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}
