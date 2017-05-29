package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class PutConsumerCheckpointHandler(server: TransactionServer,
                                   scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.PutConsumerCheckpoint
    val args = descriptor.decodeRequest(requestBody)
    val result = scheduledCommitLog.putData(
      CommitLogToBerkeleyWriter.setConsumerStateType,
      requestBody
    )
    //    logSuccessfulProcession(Descriptors.PutStream.name)
    descriptor.encodeResponse(
      TransactionService.PutConsumerCheckpoint.Result(Some(result))
    )
  }
}
