package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.ServerRoleOptions
import GetZKCheckpointGroupServerPrefixProcessor.descriptor
import com.bwsw.tstreamstransactionserver.rpc.TransactionService
import io.netty.channel.ChannelHandlerContext

private object GetZKCheckpointGroupServerPrefixProcessor {
  val descriptor = Protocol.GetZKCheckpointGroupServerPrefix
}

class GetZKCheckpointGroupServerPrefixProcessor(serverRoleOptions: ServerRoleOptions)
  extends RequestProcessor {

  override val name: String = descriptor.name
  override val id: Byte = descriptor.methodID

  private val encodedResponse =  descriptor.encodeResponse(
    TransactionService.GetZKCheckpointGroupServerPrefix.Result(
      Some(
        serverRoleOptions.checkpointGroupMasterPrefix
      ))
  )

  override protected def handle(message: Message,
                                ctx: ChannelHandlerContext): Unit = {
    ///

  }

  override protected def handleAndGetResponse(message: Message,
                                              ctx: ChannelHandlerContext): Unit = {
    val updatedMessage = scala.util.Try(encodedResponse) match {
      case scala.util.Success(response) =>
        message.copy(
          bodyLength = response.length,
          body = response
        )
      case scala.util.Failure(throwable) =>
        val response = createErrorResponse(throwable.getMessage)
        message.copy(
          bodyLength = response.length,
          body = response
        )
    }
    sendResponseToClient(updatedMessage, ctx)
  }

//  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
//    encodedResponse
//  }
//

  override def createErrorResponse(message: String): Array[Byte] = {
    throw new UnsupportedOperationException(
      s"$name method doesn't imply error at all!"
    )
  }
}
