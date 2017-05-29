package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class PutTransactionHandler(server: TransactionServer,
                            scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.PutTransaction
    val args = descriptor.decodeRequest(requestBody)
    val result = scheduledCommitLog.putData(
      CommitLogToBerkeleyWriter.putTransactionType,
      requestBody
    )
    //    logSuccessfulProcession(Descriptors.PutStream.name)
    descriptor.encodeResponse(
      TransactionService.PutTransaction.Result(Some(result))
    )
  }
}
