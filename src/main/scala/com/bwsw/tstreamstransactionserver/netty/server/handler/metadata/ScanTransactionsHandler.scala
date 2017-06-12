package com.bwsw.tstreamstransactionserver.netty.server.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class ScanTransactionsHandler (server: TransactionServer)
  extends RequestHandler{

  private val descriptor = Protocol.ScanTransactions

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
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

  override def handle(requestBody: Array[Byte]): Unit = {
    //    throw new UnsupportedOperationException(
    //      "It doesn't make any sense to scan transactions according to fire and forget policy"
    //    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.ScanTransactions.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override def getName: String = descriptor.name
}
