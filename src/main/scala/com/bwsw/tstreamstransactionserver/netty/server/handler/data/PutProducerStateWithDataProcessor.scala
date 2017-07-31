package com.bwsw.tstreamstransactionserver.netty.server.handler.data

import com.bwsw.tstreamstransactionserver.netty.{RequestMessage, Protocol}
import com.bwsw.tstreamstransactionserver.netty.server.{RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.rpc._
import PutProducerStateWithDataProcessor._
import com.bwsw.tstreamstransactionserver.netty.server.handler.AsyncClientRequestHandler
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.ExecutionContext

private object PutProducerStateWithDataProcessor {
  val descriptor = Protocol.PutProducerStateWithData
}

class PutProducerStateWithDataProcessor(server: TransactionServer,
                                        scheduledCommitLog: ScheduledCommitLog,
                                        context: ExecutionContext)
  extends AsyncClientRequestHandler(
    descriptor.methodID,
    descriptor.name,
    context) {

  private def process(requestBody: Array[Byte]): Boolean = {
    val transactionAndData = descriptor.decodeRequest(requestBody)
    val txn  = transactionAndData.transaction
    val data = transactionAndData.data
    val from = transactionAndData.from

    server.putTransactionData(
      txn.stream,
      txn.partition,
      txn.transactionID,
      data,
      from
    )

    val transaction = Transaction(
      Some(
        ProducerTransaction(
          txn.stream,
          txn.partition,
          txn.transactionID,
          txn.state,
          txn.quantity,
          txn.ttl
        )),
      None
    )

    val binaryTransaction = Protocol.PutTransaction.encodeRequest(
      TransactionService.PutTransaction.Args(transaction)
    )

    scheduledCommitLog.putData(
      RecordType.PutTransactionType.id.toByte,
      binaryTransaction
    )
  }

  override protected def fireAndForgetImplementation(message: RequestMessage): Unit = {
    process(message.body)
  }

  override protected def fireAndReplyImplementation(message: RequestMessage,
                                                    ctx: ChannelHandlerContext): Array[Byte] = {
    val response = descriptor.encodeResponse(
      TransactionService.PutProducerStateWithData.Result(
        Some(process(message.body))
      )
    )
    response
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutProducerStateWithData.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}