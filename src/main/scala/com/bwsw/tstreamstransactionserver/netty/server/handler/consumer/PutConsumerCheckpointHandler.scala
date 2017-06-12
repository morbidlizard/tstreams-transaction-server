package com.bwsw.tstreamstransactionserver.netty.server.handler.consumer

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

class PutConsumerCheckpointHandler(server: TransactionServer,
                                   scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  private val descriptor = Protocol.PutConsumerCheckpoint

  private def process(requestBody: Array[Byte]) = {
    scheduledCommitLog.putData(
      CommitLogToBerkeleyWriter.setConsumerStateType,
      requestBody
    )
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
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

  override def getName: String = descriptor.name
}
