package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class PutTransactionHandler(server: TransactionServer,
                            scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  private val descriptor = Descriptors.PutTransaction

  private def process(requestBody: Array[Byte]) = {
    scheduledCommitLog.putData(
      CommitLogToBerkeleyWriter.putTransactionType,
      requestBody
    )
  }

  override def handleAndSendResponse(requestBody: Array[Byte]): Array[Byte] = {
    val result = process(requestBody)
    //    logSuccessfulProcession(Descriptors.PutStream.name)
    descriptor.encodeResponse(
      TransactionService.PutTransaction.Result(Some(result))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    process(requestBody)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutTransaction.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

}
