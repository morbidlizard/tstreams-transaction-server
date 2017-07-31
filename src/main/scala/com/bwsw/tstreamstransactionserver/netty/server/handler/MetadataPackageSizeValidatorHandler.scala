package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportService
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

class MetadataPackageSizeValidatorHandler(nextHandler: RequestHandler,
                                          transportService: TransportService)
  extends IntermediateRequestHandler(nextHandler) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private def createError(message: RequestMessage,
                          ctx: ChannelHandlerContext) = {
    new PackageTooBigException(
      s"A size of client[${ctx.channel().remoteAddress().toString}, request id: ${message.id}] request is greater " +
        s"than maxMetadataPackageSize (${transportService.maxMetadataPackageSize})"
    )
  }

  override def process(message: RequestMessage,
                       ctx: ChannelHandlerContext,
                       error: Option[Throwable]): Unit = {
    if (error.isDefined) {
      if (message.isFireAndForgetMethod) {
        if (logger.isDebugEnabled())
          logger.debug(s"Client ${ctx.channel().remoteAddress().toString}", error.get)
      }
      else {
        nextHandler.process(message, ctx, error)
      }
    }
    else {
      val isPackageTooBig = transportService
        .isTooBigMetadataMessage(message)

      if (isPackageTooBig)
        nextHandler.process(
          message,
          ctx,
          Some(createError(message, ctx))
        )
      else
        nextHandler.process(
          message,
          ctx,
          error
        )
    }
  }
}
