package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class IsValidHandler(server: TransactionServer)
  extends RequestHandler{

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.IsValid
    val args = descriptor.decodeRequest(requestBody)
    val result = server.isValid(args.token)
    //    logSuccessfulProcession(Descriptors.GetStream.name)
    descriptor.encodeResponse(
      TransactionService.IsValid.Result(Some(result))
    )
  }
}
