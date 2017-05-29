package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class PutStreamHandler(server: TransactionServer)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.PutStream
    val args = descriptor.decodeRequest(requestBody)
    val result = server.putStream(args.name, args.partitions, args.description, args.ttl)
    //    logSuccessfulProcession(Descriptors.PutStream.name)
    descriptor.encodeResponse(
      TransactionService.PutStream.Result(Some(result))
    )
  }
}
