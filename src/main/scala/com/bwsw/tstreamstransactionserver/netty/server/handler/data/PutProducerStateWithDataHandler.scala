package com.bwsw.tstreamstransactionserver.netty.server.handler.data

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc._
import PutProducerStateWithDataHandler._
import com.bwsw.tstreamstransactionserver.netty.server.commitLogReader.Frame

private object PutProducerStateWithDataHandler {
  val descriptor = Protocol.PutProducerStateWithData
}

class PutProducerStateWithDataHandler(server: TransactionServer,
                                      scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  private def process(requestBody: Array[Byte]) = {
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
      Frame.PutTransactionType.id.toByte,
      binaryTransaction
    )
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val isPutted = process(requestBody)
    descriptor.encodeResponse(
      TransactionService.PutProducerStateWithData.Result(
        Some(isPutted)
      )
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    process(requestBody)
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
