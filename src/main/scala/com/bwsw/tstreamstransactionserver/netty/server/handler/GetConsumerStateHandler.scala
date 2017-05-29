package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class GetConsumerStateHandler (server: TransactionServer)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.GetConsumerState
    val args = descriptor.decodeRequest(requestBody)
    val result = server.getConsumerState(
      args.name,
      args.streamID,
      args.partition
    )
    //    logSuccessfulProcession(Descriptors.GetConsumerState.name)
    descriptor.encodeResponse(
      TransactionService.GetConsumerState.Result(Some(result))
    )
  }
}
