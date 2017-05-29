package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors._
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog

final class RequestHandlerChooser(server: TransactionServer,
                                  scheduledCommitLog: ScheduledCommitLog) {

  private val commitLogOffsetsHandler = new GetCommitLogOffsetsHandler(server, scheduledCommitLog)

  private val putStreamHandler = new PutStreamHandler(server)
  private val checkStreamExistsHandler = new CheckStreamExistsHandler(server)
  private val getStreamHandler = new GetStreamHandler(server)
  private val delStreamHandler = new DelStreamHandler(server)

  def chooseHandler(id: Byte, request: Array[Byte]): Array[Byte] = id match {
    case GetCommitLogOffsets.methodID => commitLogOffsetsHandler.handle(request)
    case PutStream.methodID => putStreamHandler.handle(request)
    case CheckStreamExists.methodID => checkStreamExistsHandler.handle(request)
    case GetStream.methodID => getStreamHandler.handle(request)
    case DelStream.methodID => delStreamHandler.handle(request)
  }

}
