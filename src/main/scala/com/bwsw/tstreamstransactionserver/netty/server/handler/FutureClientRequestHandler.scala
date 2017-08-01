package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}

abstract class FutureClientRequestHandler(override final val id: Byte,
                                          override final val name: String)
  extends ClientRequestHandler(id, name) {

  override final def handle(message: RequestMessage,
                            ctx: ChannelHandlerContext,
                            acc: Option[Throwable]): Unit = {
    if (message.isFireAndForgetMethod) {
      handleFireAndForgetRequest(message, ctx, acc)
    }
    else {
      handleRequest(message, ctx, acc)
    }
  }

  private def handleFireAndForgetRequest(message: RequestMessage,
                                         ctx: ChannelHandlerContext,
                                         error: Option[Throwable]) = {
    if (error.isEmpty) {
      fireAndForgetImplementation(message)
    } else {
      logUnsuccessfulProcessing(
        name,
        error.get,
        message,
        ctx
      )
    }
  }

  private def handleRequest(message: RequestMessage,
                            ctx: ChannelHandlerContext,
                            acc: Option[Throwable]) = {
    if (acc.isEmpty) {
      val (result, context) = responseImplementation(message, ctx)
      result.recover { case error =>
        logUnsuccessfulProcessing(name, error, message, ctx)
        val response =
          createErrorResponse(error.getMessage)
        sendResponseToClient(message, response, ctx)
      }(context)
    } else {
      val error = acc.get
      logUnsuccessfulProcessing(name, error, message, ctx)
      val response = createErrorResponse(error.getMessage)
      sendResponseToClient(message, response, ctx)
    }
  }

  protected def fireAndForgetImplementation(message: RequestMessage): Unit

  protected def responseImplementation(message: RequestMessage,
                                       ctx: ChannelHandlerContext): (Future[_], ExecutionContext)
}

