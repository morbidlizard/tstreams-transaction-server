package com.bwsw.tstreamstransactionserver.netty.server.handler.transport

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import GetMaxPackagesSizesHandler._
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService, TransportOptionsInfo}

import scala.concurrent.Future

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

  override def handleAndGetResponse(requestBody: Array[Byte]): Future[Array[Byte]] = {
    Future.successful {
      val transportInfo = process(requestBody)
      descriptor.encodeResponse(
        TransactionService.GetMaxPackagesSizes.Result(Some(transportInfo))
      )
    }
  }

  override def handle(requestBody: Array[Byte]): Future[Unit] = {
    Future.failed(
      throw new UnsupportedOperationException(
        "It doesn't make any sense to get max packages sizes according to fire and forget policy"
      )
    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetMaxPackagesSizes.Result(
        None //,
        //        Some(ServerException(message)
      )
    )
  }

  override def name: String = descriptor.name
  override def id: Byte = descriptor.methodID
}
