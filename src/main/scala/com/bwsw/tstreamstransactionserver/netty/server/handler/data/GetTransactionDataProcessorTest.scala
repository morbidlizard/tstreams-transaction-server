package com.bwsw.tstreamstransactionserver.netty.server.handler.data

import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
import com.bwsw.tstreamstransactionserver.netty.server.handler.test.ClientFutureRequestHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.data.GetTransactionDataProcessor.descriptor
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.ExecutionContext

private object GetTransactionDataProcessorTest
{
  val descriptor = Protocol.GetTransactionData
}


class GetTransactionDataProcessorTest(server: TransactionServer,
                                      context: ExecutionContext)
  extends ClientFutureRequestHandler(
    descriptor.methodID,
    descriptor.name,
    context
  )
{
  private def process(requestBody: Array[Byte]) = {
    val args = descriptor.decodeRequest(requestBody)
    server.getTransactionData(
      args.streamID,
      args.partition,
      args.transaction,
      args.from,
      args.to
    )
  }

  override protected def fireAndForgetImplementation(message: Message): Unit = {}

  override protected def fireAndReplyImplementation(message: Message,
                                                    ctx: ChannelHandlerContext): Unit = {
    val response = descriptor.encodeResponse(
      TransactionService.GetTransactionData.Result(
        Some(process(message.body))
      )
    )
    val responseMessage = message.copy(
      bodyLength = response.length,
      body = response
    )
    sendResponseToClient(responseMessage, ctx)

  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetTransactionData.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}
