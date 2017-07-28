package com.bwsw.tstreamstransactionserver.netty.server.handler.test
import com.bwsw.tstreamstransactionserver.netty.Message
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

abstract class ClientRequestHandler(val id: Byte,
                                    val name: String)
  extends LastRequestHandler
    with Ordered[ClientRequestHandler]
{
  def createErrorResponse(message: String): Array[Byte]

  protected final val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  protected final def logSuccessfulProcession(method: String,
                                              message: Message,
                                              ctx: ChannelHandlerContext): Unit =
    if (logger.isDebugEnabled)
      logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}: " +
        s"$method is successfully processed!")

  protected final def logUnsuccessfulProcessing(method: String,
                                                error: Throwable,
                                                message: Message,
                                                ctx: ChannelHandlerContext): Unit =
    if (logger.isDebugEnabled)
      logger.debug(s"${ctx.channel().remoteAddress().toString} request id ${message.id}: " +
        s"$method is failed while processing!", error)


  protected final def sendResponseToClient(message: Message,
                                           ctx: ChannelHandlerContext): Unit = {
    val binaryResponse = message.toByteArray
    if (ctx.channel().isActive)
      ctx.writeAndFlush(binaryResponse)
  }

  override final def compare(that: ClientRequestHandler): Int = {
    java.lang.Byte.compare(this.id, that.id)
  }
}
