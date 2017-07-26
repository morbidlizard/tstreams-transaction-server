package com.bwsw.tstreamstransactionserver.netty.server.handler.data

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.{RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc._
import PutProducerStateWithDataHandler._

import scala.concurrent.{ExecutionContext, Future}

private object PutProducerStateWithDataHandler {
  val descriptor = Protocol.PutProducerStateWithData
}

class PutProducerStateWithDataHandler(server: TransactionServer,
                                      scheduledCommitLog: ScheduledCommitLog,
                                      context: ExecutionContext)
  extends RequestHandler {

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

  override def handleAndGetResponse(requestBody: Array[Byte]): Future[Array[Byte]] = {
    Future {
      val isPutted = process(requestBody)
      descriptor.encodeResponse(
        TransactionService.PutProducerStateWithData.Result(
          Some(isPutted)
        )
      )
    }(context)
  }

  override def handle(requestBody: Array[Byte]): Future[Unit] = {
    Future{
      process(requestBody)
      ()
    }(context)
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

  override def name: String = descriptor.name

  override def id: Byte = descriptor.methodID
}
