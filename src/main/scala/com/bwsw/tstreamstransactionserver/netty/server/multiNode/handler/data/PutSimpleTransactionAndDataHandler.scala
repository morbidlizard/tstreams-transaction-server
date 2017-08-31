package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data


import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.Structure.PutTransactionsAndData
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperMaster
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.MultiNodeArgsDependentContextHandler
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.AuthenticationOptions
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data.PutSimpleTransactionAndDataHandler._
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.PutSimpleTransactionAndData
import com.bwsw.tstreamstransactionserver.rpc._
import io.netty.channel.ChannelHandlerContext
import org.apache.bookkeeper.client.BKException.Code
import org.apache.bookkeeper.client.{AsyncCallback, BKException, LedgerHandle}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, Promise}

private object PutSimpleTransactionAndDataHandler {
  val descriptor = Protocol.PutSimpleTransactionAndData
}

class PutSimpleTransactionAndDataHandler(server: TransactionServer,
                                         bookkeeperMaster: BookkeeperMaster,
                                         notifier: OpenedTransactionNotifier,
                                         authOptions: AuthenticationOptions,
                                         orderedExecutionPool: OrderedExecutionContextPool,
                                         context: ExecutionContext)
  extends MultiNodeArgsDependentContextHandler(
    descriptor.methodID,
    descriptor.name,
    orderedExecutionPool) {


  private class ReplyCallback(stream: Int,
                              partition: Int,
                              transactionId: Long,
                              count: Int,
                              status: TransactionState.Status,
                              ttlMs: Long,
                              authKey: String,
                              isNotReliable: Boolean,
                              message: RequestMessage,
                              ctx: ChannelHandlerContext)
    extends AsyncCallback.AddCallback {
    override def addComplete(bkCode: Int,
                             ledgerHandle: LedgerHandle,
                             entryId: Long,
                             obj: scala.Any): Unit = {
      val promise = obj.asInstanceOf[Promise[Boolean]]
      if (Code.OK == bkCode) {
        val response = descriptor.encodeResponse(
          TransactionService.PutSimpleTransactionAndData.Result(
            Some(transactionId)
          )
        )

        sendResponse(message, response, ctx)

        notifier.notifySubscribers(
          stream,
          partition,
          transactionId,
          count,
          status,
          Long.MaxValue,
          authKey,
          isNotReliable
        )
        promise.success(true)
      }
      else {
        println(BKException.getMessage(bkCode))
        promise.failure(BKException.create(bkCode).fillInStackTrace())
      }
    }
  }

  private class FireAndForgerCallback(stream: Int,
                                      partition: Int,
                                      transactionId: Long,
                                      count: Int,
                                      status: TransactionState.Status,
                                      ttlMs: Long,
                                      authKey: String,
                                      isNotReliable: Boolean)
    extends AsyncCallback.AddCallback {
    override def addComplete(bkCode: Int,
                             ledgerHandle: LedgerHandle,
                             entryId: Long,
                             obj: scala.Any): Unit = {
      val promise = obj.asInstanceOf[Promise[Boolean]]
      if (Code.OK == bkCode) {
        notifier.notifySubscribers(
          stream,
          partition,
          transactionId,
          count,
          status,
          Long.MaxValue,
          authKey,
          isNotReliable
        )
        promise.success(true)
      }
      else {
        println(BKException.getMessage(bkCode))
        promise.failure(BKException.create(bkCode).fillInStackTrace())
      }
    }
  }

  private def callback = new AsyncCallback.AddCallback {
    override def addComplete(bkCode: Int,
                             ledgerHandle: LedgerHandle,
                             entryId: Long,
                             obj: scala.Any): Unit = {
      val promise = obj.asInstanceOf[Promise[Boolean]]
      if (Code.OK == bkCode)
        promise.success(true)
      else
        promise.failure(BKException.create(bkCode).fillInStackTrace())

    }
  }

  private def prepareData(txn: PutSimpleTransactionAndData.Args,
                          transactionID: Long): ProducerTransactionsAndData = {
    val transactions = collection.immutable.Seq(
      ProducerTransaction(
        txn.streamID,
        txn.partition,
        transactionID,
        TransactionStates.Opened,
        txn.data.size, 3000L
      ),
      ProducerTransaction(
        txn.streamID,
        txn.partition,
        transactionID,
        TransactionStates.Checkpointed,
        txn.data.size,
        Long.MaxValue)
    )

    val producerTransactionsAndData =
      ProducerTransactionsAndData(transactions, txn.data)

    producerTransactionsAndData
  }

  private def process(txn: PutSimpleTransactionAndData.Args,
                      transactionID: Long,
                      context: ExecutionContext): Future[Boolean] = {

    val promise = Promise[Boolean]()
    Future {
      val requestBody = PutTransactionsAndData.encode(
        prepareData(
          txn,
          transactionID
        )
      )

      bookkeeperMaster.doOperationWithCurrentWriteLedger {
        case Left(throwable) =>
          promise.failure(throwable)

        case Right(ledgerHandler) =>
          val record = new Record(
            Frame.PutSimpleTransactionAndDataType.id.toByte,
            System.currentTimeMillis(),
            requestBody
          ).toByteArray

          ledgerHandler.asyncAddEntry(record, callback, promise)
      }
    }(context).flatMap(_ =>
      promise.future.recoverWith { case error: BKException => process(txn, transactionID, context) }(context)
    )(context)
  }


//  override protected def fireAndForget(message: RequestMessage): Unit = {
//    val args = descriptor.decodeRequest(message.body)
//    val context = orderedExecutionPool.pool(args.streamID, args.partition)
//    val transactionID = server.getTransactionID
//    process(args, transactionID, context).map { _ =>
//      notifier.notifySubscribers(
//        args.streamID,
//        args.partition,
//        transactionID,
//        args.data.size,
//        TransactionState.Status.Instant,
//        Long.MaxValue,
//        authOptions.key,
//        isNotReliable = true
//      )
//    }(context)
//  }

  //  override protected def getResponse(message: RequestMessage,
  //                                     ctx: ChannelHandlerContext): (Future[_], ExecutionContext) = {
  //    val args = descriptor.decodeRequest(message.body)
  //    val context = orderedExecutionPool.pool(args.streamID, args.partition)
  //    val transactionID = server.getTransactionID
  //    val result =
  //      process(args, transactionID, context).map { _ =>
  //        val response = descriptor.encodeResponse(
  //          TransactionService.PutSimpleTransactionAndData.Result(
  //            Some(transactionID)
  //          )
  //        )
  //        sendResponse(message, response, ctx)
  //
  //        notifier.notifySubscribers(
//            args.streamID,
//            args.partition,
//            transactionID,
//            args.data.size,
//            TransactionState.Status.Instant,
//            Long.MaxValue,
//            authOptions.key,
//            isNotReliable = false
  //        )
  //      }(context)
  //    (result, context)
  //  }


  override protected def fireAndForget(message: RequestMessage): Unit = {
    val txn = descriptor.decodeRequest(message.body)
//    val context = orderedExecutionPool.pool(txn.streamID, txn.partition)
    val context = this.context
    val transactionID = server.getTransactionID

    def helper(): Future[Boolean] = {
      val promise = Promise[Boolean]()
      Future {
        val requestBody = PutTransactionsAndData.encode(
          prepareData(
            txn,
            transactionID
          )
        )

        bookkeeperMaster.doOperationWithCurrentWriteLedger {
          case Left(throwable) =>
            promise.failure(throwable)

          case Right(ledgerHandler) =>
            val record = new Record(
              Frame.PutSimpleTransactionAndDataType.id.toByte,
              System.currentTimeMillis(),
              requestBody
            ).toByteArray


            val callback = new FireAndForgerCallback(
              txn.streamID,
              txn.partition,
              transactionID,
              txn.data.size,
              TransactionState.Status.Instant,
              Long.MaxValue,
              authOptions.key,
              isNotReliable = true
            )

            ledgerHandler.asyncAddEntry(record, callback, promise)
        }
      }(context).flatMap(_ =>
        promise.future.recoverWith { case error: BKException => process(txn, transactionID, context) }(context)
      )(context)
    }
    helper()
  }

  override protected def getResponse(message: RequestMessage,
                                     ctx: ChannelHandlerContext): (Future[_], ExecutionContext) = {
    val txn = descriptor.decodeRequest(message.body)
//    val context = orderedExecutionPool.pool(txn.streamID, txn.partition)
    val context = this.context
    val transactionID = server.getTransactionID

    def helper(): Future[Boolean] = {
      val promise = Promise[Boolean]()
      Future {
        val requestBody = PutTransactionsAndData.encode(
          prepareData(
            txn,
            transactionID
          )
        )

        bookkeeperMaster.doOperationWithCurrentWriteLedger {
          case Left(throwable) =>
            promise.failure(throwable)

          case Right(ledgerHandler) =>
            val record = new Record(
              Frame.PutSimpleTransactionAndDataType.id.toByte,
              System.currentTimeMillis(),
              requestBody
            ).toByteArray


            val callback = new ReplyCallback(
              txn.streamID,
              txn.partition,
              transactionID,
              txn.data.size,
              TransactionState.Status.Instant,
              Long.MaxValue,
              authOptions.key,
              isNotReliable = false,
              message,
              ctx
            )

            ledgerHandler.asyncAddEntry(record, callback, promise)
        }
      }(context).flatMap(_ =>
        promise.future.recoverWith { case error: BKException => process(txn, transactionID, context) }(context)
      )(context)
    }

    (helper(), context)
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
}


