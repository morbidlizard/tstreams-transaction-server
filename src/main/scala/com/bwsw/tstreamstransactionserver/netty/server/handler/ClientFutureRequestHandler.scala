package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}

abstract class ClientFutureRequestHandler(override final val id: Byte,
                                          override final val name: String,
                                          context: ExecutionContext)
  extends ClientRequestHandler(id, name) {

  protected def fireAndForgetImplementation(message: RequestMessage): Unit

  protected def fireAndReplyImplementation(message: RequestMessage,
                                           ctx: ChannelHandlerContext): Array[Byte]


  private def handleFireAndForgetRequest(message: RequestMessage,
                                         ctx: ChannelHandlerContext,
                                         acc: Option[Throwable]) = {
    if (acc.isEmpty) {
      Future(fireAndForgetImplementation(message))(context)
    } else {
      logUnsuccessfulProcessing(
        name,
        acc.get,
        message,
        ctx
      )
    }
  }

  private def handleFireAndReplyRequest(message: RequestMessage,
                                        ctx: ChannelHandlerContext,
                                        error: Option[Throwable]) = {
    if (error.isEmpty) {
      Future {
        val response = fireAndReplyImplementation(message, ctx)
        sendResponseToClient(message, response, ctx)
      }(context)
        .recover { case error =>
          logUnsuccessfulProcessing(name, error, message, ctx)
          val response = createErrorResponse(error.getMessage)
          sendResponseToClient(message, response, ctx)
        }(context)
    } else {
      val throwable = error.get
      logUnsuccessfulProcessing(name, throwable, message, ctx)
      val response = createErrorResponse(throwable.getMessage)
      sendResponseToClient(message, response, ctx)
    }
  }


  override final def process(message: RequestMessage,
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
