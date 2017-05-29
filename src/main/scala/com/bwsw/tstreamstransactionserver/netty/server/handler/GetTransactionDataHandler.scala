package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class GetTransactionDataHandler(server: TransactionServer)
  extends RequestHandler{

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.GetTransactionData
    val args = descriptor.decodeRequest(requestBody)
    val result = server.getTransactionData(
      args.streamID,
      args.partition,
      args.transaction,
      args.from,
      args.to
    )
    //    logSuccessfulProcession(Descriptors.GetStream.name)
    descriptor.encodeResponse(
      TransactionService.GetTransactionData.Result(Some(result))
    )
  }
}
