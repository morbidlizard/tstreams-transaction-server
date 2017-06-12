package com.bwsw.tstreamstransactionserver.netty.server.handler.stream

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class PutStreamHandler(server: TransactionServer)
  extends RequestHandler {

  private val descriptor = Protocol.PutStream

  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    server.putStream(args.name, args.partitions, args.description, args.ttl)
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val result = process(requestBody)
    //    logSuccessfulProcession(Descriptors.PutStream.name)
    descriptor.encodeResponse(
      TransactionService.PutStream.Result(Some(result))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    process(requestBody)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutStream.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override def getName: String = descriptor.name
}
