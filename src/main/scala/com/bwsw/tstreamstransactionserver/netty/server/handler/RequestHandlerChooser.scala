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


  def handler(id: Byte): RequestHandler = id match {
    case GetCommitLogOffsets.methodID =>
      commitLogOffsetsHandler

    case PutStream.methodID =>
      putStreamHandler
    case CheckStreamExists.methodID =>
      checkStreamExistsHandler
    case GetStream.methodID =>
      getStreamHandler
    case DelStream.methodID =>
      delStreamHandler

    case PutTransaction.methodID =>
      putTransactionHandler
    case PutTransactions.methodID =>
      putTransactionsHandler
    case PutSimpleTransactionAndData.methodID =>
      putSimpleTransactionAndDataHandler
    case GetTransaction.methodID =>
      getTransactionHandler
    case GetLastCheckpointedTransaction.methodID =>
      getLastCheckpointedTransaction
    case ScanTransactions.methodID =>
      scanTransactionsHandler
    case PutTransactionData.methodID =>
      putTransactionDataHandler
    case GetTransactionData.methodID =>
      getTransactionDataHandler

    case PutConsumerCheckpoint.methodID =>
      putConsumerCheckpointHandler
    case GetConsumerState.methodID =>
      getConsumerStateHandler

    case Authenticate.methodID =>
      authenticateHandler
    case IsValid.methodID =>
      isValidHandler

    case methodID =>
      throw new IllegalArgumentException(s"Not implemented method that has id: $methodID")
  }

}
