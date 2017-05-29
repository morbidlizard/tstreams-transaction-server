package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class PutTransactionDataHandler(server: TransactionServer)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.PutTransactionData
    val args = descriptor.decodeRequest(requestBody)
    val result = server.putTransactionData(
      args.streamID,
      args.partition,
      args.transaction,
      args.data,
      args.from
    )
    //    logSuccessfulProcession(Descriptors.PutStream.name)
    descriptor.encodeResponse(
      TransactionService.PutTransactionData.Result(Some(result))
    )
  }
}
