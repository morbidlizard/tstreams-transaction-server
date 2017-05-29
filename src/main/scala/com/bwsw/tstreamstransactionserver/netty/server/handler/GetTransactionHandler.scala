package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class GetTransactionHandler(server: TransactionServer)
  extends RequestHandler{

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.GetTransaction
    val args = descriptor.decodeRequest(requestBody)
    val result = server.getTransaction(args.streamID, args.partition, args.transaction)
    //    logSuccessfulProcession(Descriptors.GetStream.name)
    descriptor.encodeResponse(
      TransactionService.GetTransaction.Result(Some(result))
    )
  }
}
