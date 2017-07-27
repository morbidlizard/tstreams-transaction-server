package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.options.ServerOptions.ServerRoleOptions
import GetZKCheckpointGroupServerPrefixProcessor.descriptor
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

private object GetZKCheckpointGroupServerPrefixProcessor {
  val descriptor = Protocol.GetZKCheckpointGroupServerPrefix
}

class GetZKCheckpointGroupServerPrefixProcessor(serverRoleOptions: ServerRoleOptions)
  extends RequestProcessor {

  private val encodedResponse =  descriptor.encodeResponse(
    TransactionService.GetZKCheckpointGroupServerPrefix.Result(
      Some(
        serverRoleOptions.checkpointGroupMasterPrefix
      ))

  )

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    encodedResponse
  }

  override def handle(requestBody: Array[Byte]): Unit = {}

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException(
      s"$name method doesn't imply error at all!"
    )
  }

  override def name: String = descriptor.name
  override def id: Byte = descriptor.methodID
}
