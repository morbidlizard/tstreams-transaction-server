package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}

abstract class FutureClientRequestHandler(override final val id: Byte,
                                          override final val name: String)
  extends ClientRequestHandler(id, name) {

  protected def fireAndForgetImplementation(message: RequestMessage): Future[_]

  protected def fireAndReplyImplementation(message: RequestMessage,
                                           ctx: ChannelHandlerContext): (Future[_], ExecutionContext)


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

  private def handleFireAndReplyRequest(message: RequestMessage,
                                        ctx: ChannelHandlerContext,
                                        acc: Option[Throwable]) = {
    if (acc.isEmpty) {
      val (result, context) =
        fireAndReplyImplementation(message, ctx)
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

