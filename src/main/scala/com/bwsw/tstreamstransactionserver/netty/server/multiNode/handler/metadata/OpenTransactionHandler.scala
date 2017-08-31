package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata

import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperMaster
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.MultiNodeArgsDependentContextHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata.OpenTransactionHandler._
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.AuthenticationOptions
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc._
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.OpenTransaction
import io.netty.channel.ChannelHandlerContext
import org.apache.bookkeeper.client.BKException.Code
import org.apache.bookkeeper.client.{AsyncCallback, BKException, LedgerHandle}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, Promise}

private object OpenTransactionHandler {
  val descriptor = Protocol.OpenTransaction
}

class OpenTransactionHandler(server: TransactionServer,
                             bookkeeperMaster: BookkeeperMaster,
                             notifier: OpenedTransactionNotifier,
                             authOptions: AuthenticationOptions,
                             orderedExecutionPool: OrderedExecutionContextPool,
                             context: ExecutionContext)
  extends MultiNodeArgsDependentContextHandler(
    descriptor.methodID,
    descriptor.name,
    orderedExecutionPool) {



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
          TransactionService.OpenTransaction.Result(
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

  private class FireAndForgetCallback(stream: Int,
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


  private def process(args: OpenTransaction.Args,
                      transactionId: Long,
                      context: ExecutionContextExecutorService): Future[Boolean] = {
    val promise = Promise[Boolean]()
    Future {
      val txn = Transaction(Some(
        ProducerTransaction(
          args.streamID,
          args.partition,
          transactionId,
          TransactionStates.Opened,
          quantity = 0,
          ttl = args.transactionTTLMs
        )), None
      )

      val binaryTransaction = Protocol.PutTransaction.encodeRequest(
        TransactionService.PutTransaction.Args(txn)
      )

      bookkeeperMaster.doOperationWithCurrentWriteLedger {
        case Left(throwable) =>
          promise.failure(throwable)

        case Right(ledgerHandler) =>
          val record = new Record(
            Frame.PutTransactionType.id.toByte,
            System.currentTimeMillis(),
            binaryTransaction
          ).toByteArray

          ledgerHandler.asyncAddEntry(record, callback, promise)
      }
    }(context).flatMap(_ =>
      promise.future.recoverWith{case error:BKException => process(args, transactionId, context)}(context)
    )(context)
  }

  override protected def fireAndForget(message: RequestMessage): Unit = {
    val args = descriptor.decodeRequest(message.body)
//    val context = orderedExecutionPool.pool(args.streamID, args.partition)
    val context = this.context
    val transactionID = server.getTransactionID

    def helper(): Future[Boolean] = {
      val promise = Promise[Boolean]()
      Future {
        val txn = Transaction(Some(
          ProducerTransaction(
            args.streamID,
            args.partition,
            transactionID,
            TransactionStates.Opened,
            quantity = 0,
            ttl = args.transactionTTLMs
          )), None
        )

        val binaryTransaction = Protocol.PutTransaction.encodeRequest(
          TransactionService.PutTransaction.Args(txn)
        )

        bookkeeperMaster.doOperationWithCurrentWriteLedger {
          case Left(throwable) =>
            promise.failure(throwable)

          case Right(ledgerHandler) =>
            val record = new Record(
              Frame.PutTransactionType.id.toByte,
              System.currentTimeMillis(),
              binaryTransaction
            ).toByteArray

            val callback = new FireAndForgetCallback(
              args.streamID,
              args.partition,
              transactionID,
              count = 0,
              TransactionState.Status.Opened,
              args.transactionTTLMs,
              authOptions.key,
              isNotReliable = true
            )
            ledgerHandler.asyncAddEntry(record, callback, promise)

//            ledgerHandler.addEntry(record)
//
//            notifier.notifySubscribers(
//              args.streamID,
//              args.partition,
//              transactionID,
//              count = 0,
//              TransactionState.Status.Opened,
//              args.transactionTTLMs,
//              authOptions.key,
//              isNotReliable = true
//            )
//            promise.success(true)

        }
      }(context).flatMap(_ =>
        promise.future.recoverWith { case error: BKException => helper() }(context)
      )(context)
    }

    helper()
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
//        count = 0,
//        TransactionState.Status.Opened,
//        args.transactionTTLMs,
//        authOptions.key,
//        isNotReliable = true
//      )
//    }(context)
//  }

  override protected def getResponse(message: RequestMessage,
                                     ctx: ChannelHandlerContext): (Future[_], ExecutionContext) = {
    val args = descriptor.decodeRequest(message.body)
//    val context = orderedExecutionPool.pool(args.streamID, args.partition)
    val context = this.context
    val transactionID = server.getTransactionID

    def helper(): Future[Boolean] = {
      val promise = Promise[Boolean]()
      Future {
        val txn = Transaction(Some(
          ProducerTransaction(
            args.streamID,
            args.partition,
            transactionID,
            TransactionStates.Opened,
            quantity = 0,
            ttl = args.transactionTTLMs
          )), None
        )

        val binaryTransaction = Protocol.PutTransaction.encodeRequest(
          TransactionService.PutTransaction.Args(txn)
        )

        bookkeeperMaster.doOperationWithCurrentWriteLedger {
          case Left(throwable) =>
            promise.failure(throwable)

          case Right(ledgerHandler) =>
            val record = new Record(
              Frame.PutTransactionType.id.toByte,
              System.currentTimeMillis(),
              binaryTransaction
            ).toByteArray

            val callback = new ReplyCallback(
              args.streamID,
              args.partition,
              transactionID,
              count = 0,
              TransactionState.Status.Opened,
              args.transactionTTLMs,
              authOptions.key,
              isNotReliable = false,
              message,
              ctx
            )
            ledgerHandler.asyncAddEntry(record, callback, promise)

//          ledgerHandler.addEntry(record)
//
//            val response = descriptor.encodeResponse(
//              TransactionService.OpenTransaction.Result(
//                Some(transactionID)
//              )
//            )
//            sendResponse(message, response, ctx)
//
//            notifier.notifySubscribers(
//              args.streamID,
//              args.partition,
//              transactionID,
//              count = 0,
//              TransactionState.Status.Opened,
//              args.transactionTTLMs,
//              authOptions.key,
//              isNotReliable = false,
//            )
//            promise.success(true)

        }
      }(context).flatMap(_ =>
        promise.future.recoverWith { case error: BKException => helper() }(context)
      )(context)
    }

    (helper(), context)
  }



//  override protected def getResponse(message: RequestMessage,
//                                     ctx: ChannelHandlerContext): (Future[_], ExecutionContext) = {
//    val args = descriptor.decodeRequest(message.body)
//    val context = orderedExecutionPool.pool(args.streamID, args.partition)
//    val transactionID = server.getTransactionID
//    val result =
//      process(args, transactionID, context).map { _ =>
//        val response = descriptor.encodeResponse(
//          TransactionService.OpenTransaction.Result(
//            Some(transactionID)
//          )
//        )
//
//        sendResponse(message, response, ctx)
//
//        notifier.notifySubscribers(
//          args.streamID,
//          args.partition,
//          transactionID,
//          count = 0,
//          TransactionState.Status.Opened,
//          args.transactionTTLMs,
//          authOptions.key,
//          isNotReliable = false
//        )
//      }(context)
//    (result, context)
//  }

  override def createErrorResponse(message: String): Array[Byte] = {
    descriptor.encodeResponse(
      TransactionService.OpenTransaction.Result(
        None,
        Some(ServerException(message)
        )
      )
    )
  }
}
