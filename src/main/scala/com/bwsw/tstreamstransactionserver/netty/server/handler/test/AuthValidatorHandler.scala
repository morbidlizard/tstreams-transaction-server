package com.bwsw.tstreamstransactionserver.netty.server.handler.test
import com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException
import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

class AuthValidatorHandler(nextHandler: RequestHandler,
                           authService: AuthService)
  extends IntermidiateRequestHandler(nextHandler) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  override def process(message: Message,
                       ctx: ChannelHandlerContext,
                       acc: Option[Throwable]): Unit = {
    if (acc.isDefined) {
      if (message.isFireAndForgetMethod) {
        if (logger.isDebugEnabled())
          logger.debug(s"$message is not valid")
      }
      else {
        nextHandler.process(message, ctx, acc)
      }
    } else {
      val isValid = authService.isValid(message.token)
      if (isValid)
        nextHandler.process(message, ctx, acc)
      else {
        if (message.isFireAndForgetMethod) {
          if (logger.isDebugEnabled())
            logger.debug(s"$message is not valid")
        }
        else {
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
