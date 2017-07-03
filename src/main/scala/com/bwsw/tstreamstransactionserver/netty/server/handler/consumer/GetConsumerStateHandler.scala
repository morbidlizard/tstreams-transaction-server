package com.bwsw.tstreamstransactionserver.netty.server.handler.consumer

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class GetConsumerStateHandler (server: TransactionServer)
  extends RequestHandler {

  private val descriptor = Descriptors.GetConsumerState

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
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

  override def handle(requestBody: Array[Byte]): Unit = {
    //    throw new UnsupportedOperationException(
    //      "It doesn't make any sense to get consumer state according to fire and forget policy"
    //    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetConsumerState.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override def getName: String = descriptor.name
}
