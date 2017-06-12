package com.bwsw.tstreamstransactionserver.netty.server.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class GetTransactionHandler(server: TransactionServer)
  extends RequestHandler{

  private val descriptor = Protocol.GetTransaction

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    server.getTransaction(args.streamID, args.partition, args.transaction)
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val result = process(requestBody)
    descriptor.encodeResponse(
      TransactionService.GetTransaction.Result(Some(result))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    //    throw new UnsupportedOperationException(
    //      "It doesn't make any sense to get producer transaction according to fire and forget policy"
    //    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetTransaction.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override def getName: String = descriptor.name
}
