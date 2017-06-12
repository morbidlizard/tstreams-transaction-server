package com.bwsw.tstreamstransactionserver.netty.server.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{CommitLogInfo, ServerException, TransactionService}

class GetCommitLogOffsetsHandler(server: TransactionServer,
                                 scheduledCommitLog: ScheduledCommitLog
                                )
  extends RequestHandler {

  private val descriptor = Protocol.GetCommitLogOffsets

  private def process(requestBody: Array[Byte]) = {
    TransactionService.GetCommitLogOffsets.Result(
      Some(CommitLogInfo(
        server.getLastProcessedCommitLogFileID,
        scheduledCommitLog.currentCommitLogFile)
      )
    )
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val response = process(requestBody)
    descriptor.encodeResponse(response)
  }

  override def handle(requestBody: Array[Byte]): Unit = {
//    throw new UnsupportedOperationException(
//      "It doesn't make any sense to get commit log offsets according to fire and forget policy"
//    )
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.GetCommitLogOffsets.Result(
        None,
        Some(ServerException(message))
      )
    )
  }

  override def getName: String = descriptor.name
}
