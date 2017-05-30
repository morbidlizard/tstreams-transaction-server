package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class GetLastCheckpointedTransactionHandler (server: TransactionServer)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.GetLastCheckpointedTransaction
    val args = descriptor.decodeRequest(requestBody)
    val result = server.getLastCheckpointedTransaction(args.streamID, args.partition)
    //    logSuccessfulProcession(Descriptors.GetLastCheckpointedTransaction.name)
    descriptor.encodeResponse(
      TransactionService.GetLastCheckpointedTransaction.Result(result)
    )
  }
}
