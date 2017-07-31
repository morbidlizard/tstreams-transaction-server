package com.bwsw.tstreamstransactionserver.netty.server.handler.test

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportService
import io.netty.channel.ChannelHandlerContext
import org.slf4j.{Logger, LoggerFactory}

class MetadataPackageSizeValidatorHandler(nextHandler: RequestHandler,
                                          transportService: TransportService)
  extends IntermidiateRequestHandler(nextHandler) {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private def createError(message: Message) = {
    new PackageTooBigException(
      "A size of client request " +
        s"is greater than maxMetadataPackageSize (${transportService.maxMetadataPackageSize})"
    )
  }

  override def process(message: Message,
                       ctx: ChannelHandlerContext,
                       acc: Option[Throwable]): Unit = {
    if (acc.isDefined) {
      if (message.isFireAndForgetMethod) {
        if (logger.isDebugEnabled())
          logger.debug("Client sent big message")
      }
      else {
        nextHandler.process(message, ctx, acc)
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
          acc
        )
    }
  }
}
