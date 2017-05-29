package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class ScanTransactionsHandler (server: TransactionServer)
  extends RequestHandler{

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.ScanTransactions
    val args = descriptor.decodeRequest(requestBody)
    val result = server.scanTransactions(
      args.streamID,
      args.partition,
      args.from,
      args.to,
      args.count,
      args.states
    )
    //    logSuccessfulProcession(Descriptors.GetStream.name)
    descriptor.encodeResponse(
      TransactionService.ScanTransactions.Result(Some(result))
    )
  }
}
