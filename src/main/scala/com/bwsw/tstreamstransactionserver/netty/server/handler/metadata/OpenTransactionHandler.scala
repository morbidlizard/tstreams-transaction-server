package com.bwsw.tstreamstransactionserver.netty.server.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc._
import OpenTransactionHandler._


class OpenTransactionHandler(server: TransactionServer,
                             scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  private def process(requestBody: Array[Byte]) = {
    val transactionID = server.getTransactionID
    val args = descriptor.decodeRequest(requestBody)

    val txn = Transaction(Some(
      ProducerTransaction(
        args.streamID,
        args.partition,
        transactionID,
        TransactionStates.Opened,
        quantity = 0,
        ttl = args.transactionTTLMs
      )), None
    )

    val binaryTransaction = Protocol.PutTransaction.encodeRequest(
      TransactionService.PutTransaction.Args(txn)
    )

    scheduledCommitLog.putData(
      CommitLogToBerkeleyWriter.putTransactionType,
      binaryTransaction
    )

    transactionID
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val transactionID = process(requestBody)
    descriptor.encodeResponse(
      TransactionService.OpenTransaction.Result(Some(transactionID))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    process(requestBody)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.OpenTransaction.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override def getName: String = descriptor.name
}

private object OpenTransactionHandler {
  val descriptor = Protocol.OpenTransaction
}

