package com.bwsw.tstreamstransactionserver.netty.server.handler.transport

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import GetMaxPackagesSizesHandler._
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc.{TransactionService, TransportOptionsInfo}

private object GetMaxPackagesSizesHandler {
  val descriptor = Protocol.GetMaxPackagesSizes
}

class GetMaxPackagesSizesHandler(packageTransmissionOpts: TransportOptions)
  extends RequestHandler {

  private def process(requestBody: Array[Byte]) = {
    val response = TransportOptionsInfo(
      packageTransmissionOpts.maxMetadataPackageSize,
      packageTransmissionOpts.maxDataPackageSize
    )
    response
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val transportInfo = process(requestBody)
    descriptor.encodeResponse(
      TransactionService.GetMaxPackagesSizes.Result(Some(transportInfo))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {}

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException(
      s"$name method doesn't imply error at all!"
    )
  }

  override def name: String = descriptor.name
  override def id: Byte = descriptor.methodID
}
