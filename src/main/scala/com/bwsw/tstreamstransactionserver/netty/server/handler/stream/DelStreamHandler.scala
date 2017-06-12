package com.bwsw.tstreamstransactionserver.netty.server.handler.stream

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class DelStreamHandler(server: TransactionServer)
  extends RequestHandler{

  private val descriptor = Protocol.DelStream

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    server.delStream(args.name)
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val result = process(requestBody)
    //    logSuccessfulProcession(Descriptors.GetStream.name)
    descriptor.encodeResponse(
      TransactionService.DelStream.Result(Some(result))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    process(requestBody)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.DelStream.Result(
        None,
        Some(ServerException(message))
      )
    )

  }

  override def getName: String = descriptor.name
}
