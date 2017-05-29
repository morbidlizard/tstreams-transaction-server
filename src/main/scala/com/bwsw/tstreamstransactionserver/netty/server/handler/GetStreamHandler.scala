package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class GetStreamHandler(server: TransactionServer)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.GetStream
    val args = descriptor.decodeRequest(requestBody)
    val result = server.getStream(args.name)
    //    logSuccessfulProcession(Descriptors.GetStream.name)
    descriptor.encodeResponse(
      TransactionService.GetStream.Result(result)
    )
  }
}
