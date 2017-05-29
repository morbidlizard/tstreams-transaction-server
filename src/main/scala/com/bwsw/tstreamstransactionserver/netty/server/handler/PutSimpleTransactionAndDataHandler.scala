package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.netty.Descriptors
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, ScheduledCommitLog}
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionService, TransactionStates}

class PutSimpleTransactionAndDataHandler(server: TransactionServer,
                                         scheduledCommitLog: ScheduledCommitLog)
  extends RequestHandler {

  override def handle(requestBody: Array[Byte]): Array[Byte] = {
    val descriptor = Descriptors.PutSimpleTransactionAndData
    val txn = descriptor.decodeRequest(requestBody)
    server.putTransactionData(txn.streamID, txn.partition, txn.transaction, txn.data, 0)
    val transactions = collection.immutable.Seq(
      Transaction(Some(ProducerTransaction(txn.streamID, txn.partition, txn.transaction, TransactionStates.Opened, txn.data.size, 3L)), None),
      Transaction(Some(ProducerTransaction(txn.streamID, txn.partition, txn.transaction, TransactionStates.Checkpointed, txn.data.size, 120L)), None)
    )
    val messageForPutTransactions = Descriptors.PutTransactions.encodeRequest(TransactionService.PutTransactions.Args(transactions))
    val isPutted = scheduledCommitLog.putData(CommitLogToBerkeleyWriter.putTransactionsType, messageForPutTransactions)
//    logSuccessfulProcession(Descriptors.PutSimpleTransactionAndData.name)
    Descriptors.PutSimpleTransactionAndData.encodeResponse(TransactionService.PutSimpleTransactionAndData.Result(Some(isPutted)))
  }
}
