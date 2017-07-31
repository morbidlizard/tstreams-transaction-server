package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportService
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

class DataPackageSizeValidatorHandler(nextHandler: RequestHandler,
                                      transportService: TransportService)
  extends IntermediateRequestHandler(nextHandler) {

  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private def createError(message: RequestMessage) = {
    new PackageTooBigException(
      "A size of client request is greater " +
        s"than maxDataPackageSize (${transportService.maxDataPackageSize})"
    )
  }

  override def process(message: RequestMessage,
                       ctx: ChannelHandlerContext,
                       error: Option[Throwable]): Unit = {
    if (error.isDefined) {
      if (message.isFireAndForgetMethod) {
        if (logger.isDebugEnabled())
          logger.debug("Client sent big message")
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
          Some(createError(message))
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
