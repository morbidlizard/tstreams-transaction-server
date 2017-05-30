package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc.{AuthInfo, TransactionService}

class AuthenticateHandler(server: TransactionServer,
                          packageTransmissionOpts: TransportOptions)
  extends RequestHandler{

  private val descriptor = Descriptors.Authenticate

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    val result = server.authenticate(args.authKey)
    AuthInfo(result,
      packageTransmissionOpts.maxMetadataPackageSize,
      packageTransmissionOpts.maxDataPackageSize
    )
  }

  override def handleAndSendResponse(requestBody: Array[Byte]): Array[Byte] = {
    val authInfo = process(requestBody)
    //    logSuccessfulProcession(Descriptors.GetStream.name)
    descriptor.encodeResponse(
      TransactionService.Authenticate.Result(Some(authInfo))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
//    throw new UnsupportedOperationException(
//      "It doesn't make any sense to authenticate to fire and forget policy"
//    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException(
      "Authenticate method doesn't imply error at all!"
    )
  }

}
