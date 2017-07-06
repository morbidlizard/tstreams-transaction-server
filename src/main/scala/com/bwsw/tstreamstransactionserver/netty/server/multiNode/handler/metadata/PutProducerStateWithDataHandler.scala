package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
import com.bwsw.tstreamstransactionserver.netty.server.{RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.RequestHandler
import io.netty.channel.ChannelHandlerContext
import com.bwsw.tstreamstransactionserver.rpc._
import PutSimpleTransactionAndDataHandler._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookKeeperGateway
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import org.apache.bookkeeper.client.{AsyncCallback, BKException, LedgerHandle}

private object PutSimpleTransactionAndDataHandler {
  val protocol = Protocol.PutSimpleTransactionAndData

  val fireAndForgetCallback = new AsyncCallback.AddCallback {
    override def addComplete(operationCode: Int,
                             ledgerHandle: LedgerHandle,
                             recordID: Long,
                             ctx: scala.Any): Unit = {}
  }
}


class PutSimpleTransactionAndData(server: TransactionServer,
                                  gateway: BookKeeperGateway)
  extends RequestHandler
{
  private def process(requestBody: Array[Byte],
                      callback: AsyncCallback.AddCallback) = {
    val transactionID = server.getTransactionID
    val txn = protocol.decodeRequest(requestBody)
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
    val messageForPutTransactions =
      Protocol.PutTransactions.encodeRequest(
        TransactionService.PutTransactions.Args(transactions)
      )

    gateway.doOperationWithCurrentWriteLedger { currentLedger =>
      val record = new Record(
        RecordType.PutTransactionsType,
        System.currentTimeMillis(),
        messageForPutTransactions
      )

      currentLedger.asyncAddEntry(
        record.toByteArray,
        callback,
        transactionID
      )
    }
  }

  override def getName: String = protocol.name

  override def handleAndSendResponse(requestBody: Array[Byte],
                                     message: Message,
                                     connection: ChannelHandlerContext): Unit = {

    val callback = new AsyncCallback.AddCallback {
      override def addComplete(operationCode: Int,
                               ledgerHandle: LedgerHandle,
                               recordID: Long,
                               transactionIDCtx: scala.Any): Unit = {
        val transactionID = transactionIDCtx.asInstanceOf[scala.Long]
        val response =
          if (BKException.Code.OK == operationCode) {
            val response = protocol.encodeResponse(
              TransactionService.PutSimpleTransactionAndData.Result(
                Some(transactionID)
              )
            )
            message.copy(
              length = response.length,
              body = response
            )
          }
          else {
            val response =
              createErrorResponse(BKException.getMessage(operationCode))
            message.copy(
              length = response.length,
              body = response
            )
          }
        connection.writeAndFlush(response.toByteArray)
      }
    }

    process(requestBody, callback)
  }

  override def handleFireAndForget(requestBody: Array[Byte]): Unit = {
    process(requestBody, fireAndForgetCallback)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    protocol.encodeResponse(
      TransactionService.PutSimpleTransactionAndData.Result(
        None,
        Some(ServerException(message))
      )
    )
  }
}
