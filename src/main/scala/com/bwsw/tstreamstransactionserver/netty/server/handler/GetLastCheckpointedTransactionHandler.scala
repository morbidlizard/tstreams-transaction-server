package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class GetLastCheckpointedTransactionHandler (server: TransactionServer)
  extends RequestHandler {

  private val descriptor = Descriptors.GetLastCheckpointedTransaction

  override def handleAndSendResponse(requestBody: Array[Byte]): Array[Byte] = {
    val args = descriptor.decodeRequest(requestBody)
    val result = server.getLastCheckpointedTransaction(args.streamID, args.partition)
    //    logSuccessfulProcession(Descriptors.GetLastCheckpointedTransaction.name)
    descriptor.encodeResponse(
      TransactionService.GetLastCheckpointedTransaction.Result(result)
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    //    throw new UnsupportedOperationException(
    //      "It doesn't make any sense to get last checkpointed state according to fire and forget policy"
    //    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetLastCheckpointedTransaction.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

}
