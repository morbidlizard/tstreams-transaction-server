package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.rpc.{CommitLogInfo, TransactionService}

class GetCommitLogOffsetsHandler(server: TransactionServer,
                                 scheduledCommitLog: ScheduledCommitLog
                                )
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.GetCommitLogOffsets
    val response = TransactionService.GetCommitLogOffsets.Result(
      Some(CommitLogInfo(
        server.getLastProcessedCommitLogFileID,
        scheduledCommitLog.currentCommitLogFile)
      )
    )
    descriptor.encodeResponse(response)
  }
}
