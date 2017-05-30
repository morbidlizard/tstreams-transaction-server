package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class PutConsumerCheckpointHandler(server: TransactionServer,
                                   scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  private val descriptor = Descriptors.PutConsumerCheckpoint

  private def process(requestBody: Array[Byte]) = {
    scheduledCommitLog.putData(
      CommitLogToBerkeleyWriter.setConsumerStateType,
      requestBody
    )
  }

  override def handleAndSendResponse(requestBody: Array[Byte]): Array[Byte] = {
    val result = process(requestBody)
    //    logSuccessfulProcession(Descriptors.PutStream.name)
    descriptor.encodeResponse(
      TransactionService.PutConsumerCheckpoint.Result(Some(result))
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    process(requestBody)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutConsumerCheckpoint.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}
