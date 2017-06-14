package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.BookkeeperGateway
import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.record.{Record, RecordType}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}
import io.netty.channel.ChannelHandlerContext
import org.apache.bookkeeper.client.{AsyncCallback, BKException, LedgerHandle}
import PutTransactionHandler._

private object PutTransactionHandler {
  val protocol = Protocol.PutTransaction
  val isPuttedResponse: Array[Byte] = protocol.encodeResponse(
    TransactionService.PutTransaction.Result(Some(true))
  )
  val isNotPuttedResponse: Array[Byte] = protocol.encodeResponse(
    TransactionService.PutTransaction.Result(Some(false))
  )

  val fireAndForgetCallback = new AsyncCallback.AddCallback {
    override def addComplete(operationCode: Int,
                             ledgerHandle: LedgerHandle,
                             recordID: Long,
                             ctx: scala.Any): Unit = {}
  }
}

class PutTransactionHandler(server: TransactionServer,
                            gateway: BookkeeperGateway)
  extends RequestHandler
{

  private def process(requestBody: Array[Byte],
                      callback: AsyncCallback.AddCallback) = {
    val ledger = gateway.currentLedgerHandle.get

    val record = new Record(
      RecordType.TransactionSeq,
      System.currentTimeMillis(),
      requestBody
    )

    ledger.asyncAddEntry(
      record.toByteArray,
      callback,
      null
    )
  }

  override def getName: String = protocol.name

  override def handleAndSendResponse(requestBody: Array[Byte],
                                     message: Message,
                                     connection: ChannelHandlerContext): Unit = {
    val callback = new AsyncCallback.AddCallback {
      override def addComplete(operationCode: Int,
                               ledgerHandle: LedgerHandle,
                               recordID: Long,
                               ctx: scala.Any): Unit = {
        val messageResponse =
          if (BKException.Code.OK == operationCode) {
            message.copy(
              length = isPuttedResponse.length,
              body = isPuttedResponse
            )
          }
          else {
            message.copy(
              length = isNotPuttedResponse.length,
              body = isNotPuttedResponse
            )
          }
        connection.writeAndFlush(messageResponse.toByteArray)
      }
    }

    process(requestBody, callback)
  }

  override def handleFireAndForget(requestBody: Array[Byte]): Unit = {
    process(requestBody, fireAndForgetCallback)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    protocol.encodeResponse(
      TransactionService.PutTransaction.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}
