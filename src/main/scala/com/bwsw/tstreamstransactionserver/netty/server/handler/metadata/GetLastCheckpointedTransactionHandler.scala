package com.bwsw.tstreamstransactionserver.netty.server.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class GetLastCheckpointedTransactionHandler(server: TransactionServer)
  extends RequestHandler {

  private val descriptor = Descriptors.GetLastCheckpointedTransaction

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    server.getLastCheckpointedTransaction(args.streamID, args.partition)
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val result = process(requestBody)
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


  override def getName: String = descriptor.name
}
