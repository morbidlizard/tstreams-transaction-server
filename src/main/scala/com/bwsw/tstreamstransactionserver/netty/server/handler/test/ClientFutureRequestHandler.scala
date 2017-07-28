package com.bwsw.tstreamstransactionserver.netty.server.handler.test
import com.bwsw.tstreamstransactionserver.netty.Message
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}

abstract class ClientFutureRequestHandler(override final val id: Byte,
                                          override final val name: String,
                                          context: ExecutionContext)
  extends ClientRequestHandler(id, name) {

  protected def fireAndForgetImplementation(message: Message): Unit

  protected def fireAndReplyImplementation(message: Message,
                                           ctx: ChannelHandlerContext): Unit


  private def handleFireAndForgetRequest(message: Message,
                                         ctx: ChannelHandlerContext,
                                         acc: Option[Throwable]) = {
    if (acc.isEmpty) {
      fireAndForgetImplementation(message)
    } else {
      logUnsuccessfulProcessing(
        name,
        acc.get,
        message,
        ctx
      )
    }
  }

  private def handleFireAndReplyRequest(message: Message,
                                        ctx: ChannelHandlerContext,
                                        acc: Option[Throwable]) = {
    if (acc.isEmpty) {
      Future {
        fireAndReplyImplementation(message, ctx)
      }(context)
        .recover { case error =>
          logUnsuccessfulProcessing(name, error, message, ctx)
          val response = createErrorResponse(error.getMessage)
          val responseMessage = message.copy(
            bodyLength = response.length,
            body = response
          )
          sendResponseToClient(responseMessage, ctx)
        }(context)
    } else {
      val error = acc.get
      logUnsuccessfulProcessing(name, error, message, ctx)
      val response = createErrorResponse(error.getMessage)
      val responseMessage = message.copy(
        bodyLength = response.length,
        body = response
      )
      sendResponseToClient(responseMessage, ctx)
    }
  }


  override final def process(message: Message,
                             ctx: ChannelHandlerContext,
                             acc: Option[Throwable]): Unit = {
    if (message.isFireAndForgetMethod) {
      handleFireAndForgetRequest(message, ctx, acc)
    }
    else {
      handleFireAndReplyRequest(message, ctx, acc)
    }
  }
}
