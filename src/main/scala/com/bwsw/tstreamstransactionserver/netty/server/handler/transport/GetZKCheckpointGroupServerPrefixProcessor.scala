package com.bwsw.tstreamstransactionserver.netty.server.handler.transport

import com.bwsw.tstreamstransactionserver.netty.server.handler.test.ClientFireAndForgetReadHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.GetZKCheckpointGroupServerPrefixProcessor.descriptor
import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
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
  override protected def fireAndReplyImplementation(message: Message,
                                                    ctx: ChannelHandlerContext,
                                                    acc: Option[Throwable]): Unit = {
    val updatedMessage =
      message.copy(
        bodyLength = encodedResponse.length,
        body = encodedResponse
      )
    sendResponseToClient(updatedMessage, ctx)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException(
      s"$name method doesn't imply error at all!"
    )
  }
}
