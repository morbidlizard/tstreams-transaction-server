package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.rpc.{AuthInfo, TransactionService}

class AuthenticateHandler(server: TransactionServer,
                          packageTransmissionOpts: TransportOptions)
  extends RequestHandler{

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.Authenticate
    val args = descriptor.decodeRequest(requestBody)
    val result = server.authenticate(args.authKey)
    val authInfo = AuthInfo(result,
      packageTransmissionOpts.maxMetadataPackageSize,
      packageTransmissionOpts.maxDataPackageSize
    )
    //    logSuccessfulProcession(Descriptors.GetStream.name)
    descriptor.encodeResponse(
      TransactionService.Authenticate.Result(Some(authInfo))
    )
  }
}
