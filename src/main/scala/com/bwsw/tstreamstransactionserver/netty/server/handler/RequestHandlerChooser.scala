package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors._
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions

final class RequestHandlerChooser(server: TransactionServer,
                                  scheduledCommitLog: ScheduledCommitLog,
                                  packageTransmissionOpts: TransportOptions
                                 ) {

  private val commitLogOffsetsHandler =
    new GetCommitLogOffsetsHandler(server, scheduledCommitLog)

  private val putStreamHandler =
    new PutStreamHandler(server)
  private val checkStreamExistsHandler =
    new CheckStreamExistsHandler(server)
  private val getStreamHandler =
    new GetStreamHandler(server)
  private val delStreamHandler =
    new DelStreamHandler(server)

  private val putTransactionHandler =
    new PutTransactionHandler(server, scheduledCommitLog)
  private val putTransactionsHandler =
    new PutTransactionsHandler(server, scheduledCommitLog)
  private val putSimpleTransactionAndDataHandler =
    new PutSimpleTransactionAndDataHandler(server, scheduledCommitLog)
  private val getTransactionHandler =
    new GetTransactionHandler(server)
  private val getLastCheckpointedTransaction =
    new GetLastCheckpointedTransactionHandler(server)
  private val scanTransactionsHandler =
    new ScanTransactionsHandler(server)
  private val putTransactionDataHandler =
    new PutTransactionDataHandler(server)
  private val getTransactionDataHandler =
    new GetTransactionDataHandler(server)

  private val putConsumerCheckpointHandler =
    new PutConsumerCheckpointHandler(server, scheduledCommitLog)
  private val getConsumerStateHandler =
    new GetConsumerStateHandler(server)

  private val authenticateHandler =
    new AuthenticateHandler(server, packageTransmissionOpts)
  private val isValidHandler =
    new IsValidHandler(server)


  def chooseHandler(id: Byte, request: Array[Byte]): Array[Byte] = id match {
    case GetCommitLogOffsets.methodID =>
      commitLogOffsetsHandler.handle(request)

    case PutStream.methodID =>
      putStreamHandler.handle(request)
    case CheckStreamExists.methodID =>
      checkStreamExistsHandler.handle(request)
    case GetStream.methodID =>
      getStreamHandler.handle(request)
    case DelStream.methodID =>
      delStreamHandler.handle(request)

    case PutTransaction.methodID =>
      putTransactionHandler.handle(request)
    case PutTransactions.methodID =>
      putTransactionsHandler.handle(request)
    case PutSimpleTransactionAndData.methodID =>
      putSimpleTransactionAndDataHandler.handle(request)
    case GetTransaction.methodID =>
      getTransactionHandler.handle(request)
    case GetLastCheckpointedTransaction.methodID =>
      getLastCheckpointedTransaction.handle(request)
    case ScanTransactions.methodID =>
      scanTransactionsHandler.handle(request)
    case PutTransactionData.methodID =>
      putTransactionDataHandler.handle(request)
    case GetTransactionData.methodID =>
      getTransactionDataHandler.handle(request)

    case PutConsumerCheckpoint.methodID =>
      putConsumerCheckpointHandler.handle(request)
    case GetConsumerState.methodID =>
      getConsumerStateHandler.handle(request)

    case Authenticate.methodID =>
      authenticateHandler.handle(request)
    case IsValid.methodID =>
      isValidHandler.handle(request)

    case methodID =>
      throw new IllegalArgumentException(s"Not implemented method that has id: $methodID")
  }

}
