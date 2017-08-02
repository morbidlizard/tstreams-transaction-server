package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata



import com.bwsw.tstreamstransactionserver.netty.server.multiNode.RequestHandler
import com.bwsw.tstreamstransactionserver.netty.server.commitLogReader.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookKeeperGateway
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata.PutSimpleTransactionAndDataHandler._
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.rpc._
import io.netty.channel.ChannelHandlerContext
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
  extends RequestHandler {
  override def getName: String = protocol.name

  override def handleAndSendResponse(requestBody: Array[Byte],
                                     message: RequestMessage,
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
              bodyLength = response.length,
              body = response
            )
          }
          else {
            val response =
              createErrorResponse(BKException.getMessage(operationCode))
            message.copy(
              bodyLength = response.length,
              body = response
            )
          }
        connection.writeAndFlush(response.toByteArray)
      }
    }

    process(requestBody, callback)
  }

  override def createErrorResponse(message: String): Array[Byte] = {
    protocol.encodeResponse(
      TransactionService.PutSimpleTransactionAndData.Result(
        None,
        Some(ServerException(message))
      )
    )
  }

  override def handleFireAndForget(requestBody: Array[Byte]): Unit = {
    process(requestBody, fireAndForgetCallback)
  }

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
        Frame.PutTransactionsType.id.toByte,
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
}
