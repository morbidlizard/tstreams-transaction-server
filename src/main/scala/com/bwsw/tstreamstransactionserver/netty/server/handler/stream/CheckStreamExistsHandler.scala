package com.bwsw.tstreamstransactionserver.netty.server.handler.stream

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class CheckStreamExistsHandler(server: TransactionServer)
  extends RequestHandler {

  private val descriptor = Descriptors.CheckStreamExists

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    server.checkStreamExists(args.name)
  }

  override def handleAndSendResponse(requestBody: Array[Byte]): Array[Byte] = {
    val result = process(requestBody)
    //    logSuccessfulProcession(Descriptors.CheckStreamExists.name)
    descriptor.encodeResponse(
      TransactionService.CheckStreamExists.Result(Some(result))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
//    throw new UnsupportedOperationException(
//      "It doesn't make any sense to check if stream exists according to fire and forget policy"
//    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.CheckStreamExists.Result(
        None,
        Some(ServerException(message))
      )
    )
  }

  override def getName: String = descriptor.name
}
