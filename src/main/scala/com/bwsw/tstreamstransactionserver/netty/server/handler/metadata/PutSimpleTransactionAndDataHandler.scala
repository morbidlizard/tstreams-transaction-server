package com.bwsw.tstreamstransactionserver.netty.server.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestHandler
import com.bwsw.tstreamstransactionserver.rpc._

class PutSimpleTransactionAndDataHandler(server: TransactionServer,
                                         scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  private val descriptor = Protocol.PutSimpleTransactionAndData

  private def process(requestBody: Array[Byte]) = {
    val transactionID = server.getTransactionID
    val txn = descriptor.decodeRequest(requestBody)
    server.putTransactionData(
      txn.streamID,
      txn.partition,
      transactionID,
      txn.data,
      0
    )

    val transactions = collection.immutable.Seq(
      Transaction(Some(
        ProducerTransaction(
          txn.streamID,
          txn.partition,
          transactionID,
          TransactionStates.Opened,
          txn.data.size, 3L
        )), None
      ),
      Transaction(Some(
        ProducerTransaction(
          txn.streamID,
          txn.partition,
          transactionID,
          TransactionStates.Checkpointed,
          txn.data.size,
          120L)), None
      )
    )
    val messageForPutTransactions = Protocol.PutTransactions.encodeRequest(
      TransactionService.PutTransactions.Args(transactions)
    )

    scheduledCommitLog.putData(
      CommitLogToBerkeleyWriter.putTransactionsType,
      messageForPutTransactions
    )
    transactionID
  }

  override def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte] = {
    val transactionID = process(requestBody)
//    logSuccessfulProcession(Descriptors.PutSimpleTransactionAndData.name)
    Protocol.PutSimpleTransactionAndData.encodeResponse(
      TransactionService.PutSimpleTransactionAndData.Result(
        Some(transactionID)
      )
    )
  }

  override def handle(requestBody: Array[Byte]): Unit = {
    process(requestBody)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.PutSimpleTransactionAndData.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }

  override def getName: String = descriptor.name
}
