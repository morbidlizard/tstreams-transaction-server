package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

class PutTransactionsHandler(server: TransactionServer,
                             scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.PutTransactions
    val args = descriptor.decodeRequest(requestBody)
    val result = scheduledCommitLog.putData(
      CommitLogToBerkeleyWriter.putTransactionsType,
      requestBody
    )
    //    logSuccessfulProcession(Descriptors.PutStream.name)
    descriptor.encodeResponse(
      TransactionService.PutTransactions.Result(Some(result))
    )
  }
}
