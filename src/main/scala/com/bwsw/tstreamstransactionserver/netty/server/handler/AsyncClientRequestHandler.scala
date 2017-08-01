package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}

abstract class AsyncClientRequestHandler(override final val id: Byte,
                                         override final val name: String,
                                         context: ExecutionContext)
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

  private def handleRequest(message: RequestMessage,
                            ctx: ChannelHandlerContext,
                            error: Option[Throwable]) = {
    if (error.isEmpty) {
      Future {
        val response = responseImplementation(message, ctx)
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

  protected def fireAndForgetImplementation(message: RequestMessage): Unit

  protected def responseImplementation(message: RequestMessage,
                                       ctx: ChannelHandlerContext): Array[Byte]
}
