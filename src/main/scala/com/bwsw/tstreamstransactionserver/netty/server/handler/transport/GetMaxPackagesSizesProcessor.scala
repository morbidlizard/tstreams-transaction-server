package com.bwsw.tstreamstransactionserver.netty.server.handler.transport

import com.bwsw.tstreamstransactionserver.netty.{RequestMessage, Protocol}
import GetMaxPackagesSizesProcessor._
import com.bwsw.tstreamstransactionserver.netty.server.handler.ClientFireAndForgetReadHandler
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc.{TransactionService, TransportOptionsInfo}
import io.netty.channel.ChannelHandlerContext


private object GetMaxPackagesSizesProcessor {
  val descriptor = Protocol.GetMaxPackagesSizes
}

class GetMaxPackagesSizesProcessor(packageTransmissionOpts: TransportOptions)
  extends ClientFireAndForgetReadHandler(
    descriptor.methodID,
    descriptor.name
  ){

  private def process(requestBody: Array[Byte]) = {
    val response = TransportOptionsInfo(
      packageTransmissionOpts.maxMetadataPackageSize,
      packageTransmissionOpts.maxDataPackageSize
    )
    response
  }

  override protected def fireAndReplyImplementation(message: RequestMessage,
                                                    ctx: ChannelHandlerContext,
                                                    error: Option[Throwable]): Array[Byte] = {
    scala.util.Try(process(message.body)) match {
      case scala.util.Success(result) =>
        val response = descriptor.encodeResponse(
          TransactionService.GetMaxPackagesSizes.Result(Some(result))
        )
        response
      case scala.util.Failure(throwable) =>
        val response = createErrorResponse(throwable.getMessage)
        response
    }
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException("IsValid method doesn't imply error at all!")
  }
}
