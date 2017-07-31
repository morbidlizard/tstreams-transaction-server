package com.bwsw.tstreamstransactionserver.netty.server.handler.transport

import com.bwsw.tstreamstransactionserver.netty.server.handler.ClientFireAndForgetReadHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.GetZKCheckpointGroupServerPrefixProcessor.descriptor
import com.bwsw.tstreamstransactionserver.netty.{RequestMessage, Protocol}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.ServerRoleOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionService
import io.netty.channel.ChannelHandlerContext

private object GetZKCheckpointGroupServerPrefixProcessor {
  val descriptor = Protocol.GetZKCheckpointGroupServerPrefix
}

class GetZKCheckpointGroupServerPrefixProcessor(serverRoleOptions: ServerRoleOptions)
  extends ClientFireAndForgetReadHandler(
    descriptor.methodID,
    descriptor.name
  ){

  private val encodedResponse =  descriptor.encodeResponse(
    TransactionService.GetZKCheckpointGroupServerPrefix.Result(
      Some(
        serverRoleOptions.checkpointGroupMasterPrefix
      ))
  )
  override protected def fireAndReplyImplementation(message: RequestMessage,
                                                    ctx: ChannelHandlerContext,
                                                    error: Option[Throwable]): Array[Byte] = {
    encodedResponse
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException(
      s"$name method doesn't imply error at all!"
    )
  }
}
