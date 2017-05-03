package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.StreamCache
import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction

import scala.concurrent.ExecutionContext

trait ConsumerTransactionStateNotifier extends StreamCache {
  private implicit lazy val notifierConsumerContext: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
  private val consumerNotifies = new java.util.concurrent.ConcurrentHashMap[Long, ConsumerTransactionNotification](0)
  private lazy val consumerNotifierSeq = new AtomicLong(0L)



  final def notifyConsumerTransactionCompleted(onNotificationCompleted: ConsumerTransaction => Boolean, func: => Unit): Long = {
    val consumerNotification = new ConsumerTransactionNotification(onNotificationCompleted, scala.concurrent.Promise[Unit]())
    val id = consumerNotifierSeq.getAndIncrement()

    consumerNotifies.put(id, consumerNotification)

    consumerNotification.notificationPromise.future.map{ onCompleteSuccessfully =>
      consumerNotifies.remove(id)
      func
    }
    id
  }

  final def removeConsumerTransactionNotification(id :Long): Boolean = consumerNotifies.remove(id) != null

  private[consumerService] final def areThereAnyConsumerNotifies = !consumerNotifies.isEmpty

  private[consumerService] final def tryCompleteConsumerNotify: ConsumerTransactionRecord => Unit => Unit = {consumerTransactionRecord => _ =>
    scala.util.Try(getStreamObjByID(consumerTransactionRecord.stream)) match {
      case scala.util.Success(stream) =>
        consumerNotifies.values().forEach(notify =>
          if (notify.notifyOn(
            ConsumerTransaction(stream.name, consumerTransactionRecord.partition, consumerTransactionRecord.transactionID, consumerTransactionRecord.name)
          )) notify.notificationPromise.trySuccess(value = Unit))
      case _ =>
    }
  }
}

private class ConsumerTransactionNotification(val notifyOn: ConsumerTransaction => Boolean,
                                              val notificationPromise: scala.concurrent.Promise[Unit])

