package com.bwsw.tstreamstransactionserver.netty.server.handler.data

import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
import com.bwsw.tstreamstransactionserver.netty.server.{RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.SomeNameRequestProcessor
import com.bwsw.tstreamstransactionserver.rpc._
import PutProducerStateWithDataProcessor._
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportService
import io.netty.channel.ChannelHandlerContext

import scala.concurrent.{ExecutionContext, Future}

private object PutProducerStateWithDataProcessor {
  val descriptor = Protocol.PutProducerStateWithData
}

class PutProducerStateWithDataProcessor(server: TransactionServer,
                                        scheduledCommitLog: ScheduledCommitLog,
                                        context: ExecutionContext,
                                        authService: AuthService,
                                        transportService: TransportService)
  extends SomeNameRequestProcessor(
    authService,
    transportService) {

  override val name: String = descriptor.name

  override val id: Byte = descriptor.methodID



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

  override protected def handleAndGetResponse(message: Message,
                                              ctx: ChannelHandlerContext): Unit = {
    val exceptionOpt = validate(message, ctx)
    if (exceptionOpt.isEmpty) {
      Future {
        val response = descriptor.encodeResponse(
          TransactionService.PutProducerStateWithData.Result(
            Some(process(message.body))
          )
        )
        val responseMessage = message.copy(
          bodyLength = response.length,
          body = response
        )
        sendResponseToClient(responseMessage, ctx)
      }(context)
        .recover { case error =>
          logUnsuccessfulProcessing(name, error, message, ctx)
          val response = createErrorResponse(error.getMessage)
          val responseMessage = message.copy(
            bodyLength = response.length,
            body = response
          )
          sendResponseToClient(responseMessage, ctx)
        }(context)
    } else {
      val error = exceptionOpt.get
      logUnsuccessfulProcessing(name, error, message, ctx)
      val response = createErrorResponse(error.getMessage)
      val responseMessage = message.copy(
        bodyLength = response.length,
        body = response
      )
      sendResponseToClient(responseMessage, ctx)
    }
  }

  override protected def handle(message: Message,
                                ctx: ChannelHandlerContext): Unit = {
    val exceptionOpt = validate(message, ctx)
    if (exceptionOpt.isEmpty) {
      process(message.body)
    }
    else {
      logUnsuccessfulProcessing(
        name,
        transportService.packageTooBigException,
        message,
        ctx
      )
    }
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
