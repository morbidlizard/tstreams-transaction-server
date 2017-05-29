package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class CheckStreamExistsHandler(server: TransactionServer)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.CheckStreamExists
    val args = descriptor.decodeRequest(requestBody)
    val result = server.checkStreamExists(args.name)
    //    logSuccessfulProcession(Descriptors.CheckStreamExists.name)
    descriptor.encodeResponse(
      TransactionService.CheckStreamExists.Result(Some(result))
    )
  }
}
