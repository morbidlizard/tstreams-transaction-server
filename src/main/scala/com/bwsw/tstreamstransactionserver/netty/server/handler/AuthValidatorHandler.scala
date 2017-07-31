package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

class AuthValidatorHandler(nextHandler: RequestHandler,
                           authService: AuthService)
  extends IntermediateRequestHandler(nextHandler) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  override def process(message: RequestMessage,
                       ctx: ChannelHandlerContext,
                       acc: Option[Throwable]): Unit = {
    if (acc.isDefined) {
      if (message.isFireAndForgetMethod) {
        if (logger.isDebugEnabled())
          logger.debug(s"Client [${ctx.channel().remoteAddress().toString}, " +
            s"request id: ${message.id}]", acc.get)
      }
      else {
        nextHandler.process(message, ctx, acc)
      }
    } else {
      val isValid = authService.isValid(message.token)
      if (isValid)
        nextHandler.process(message, ctx, acc)
      else {
        if (logger.isDebugEnabled())
          logger.debug(s"Client [${ctx.channel().remoteAddress().toString}, request id: ${message.id}] " +
            s"Token ${message.token} is not valid")
        if (!message.isFireAndForgetMethod) {
          nextHandler.process(
            message,
            ctx,
            Some(new TokenInvalidException)
          )
        }
      }
    }
  }
}
