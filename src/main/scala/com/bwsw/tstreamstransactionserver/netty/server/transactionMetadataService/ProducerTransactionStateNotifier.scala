package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.StreamCache
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction

import scala.concurrent.ExecutionContext

trait ProducerTransactionStateNotifier extends StreamCache {
  private implicit lazy val notifierProducerContext: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
  private val producerNotifies = new java.util.concurrent.ConcurrentHashMap[Long, ProducerTransactionNotification](0)
  private lazy val producerSeq = new AtomicLong(0L)



  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean, func: => Unit): Long = {
    val producerNotification = new ProducerTransactionNotification(onNotificationCompleted, scala.concurrent.Promise[Unit]())
    val id = producerSeq.getAndIncrement()

    producerNotifies.put(id, producerNotification)

    producerNotification.notificationPromise.future.map{ onCompleteSuccessfully =>
      producerNotifies.remove(id)
      func
    }
    id
  }

  final def removeProducerTransactionNotification(id :Long): Boolean = producerNotifies.remove(id) != null

  private[transactionMetadataService] final def areThereAnyProducerNotifies = !producerNotifies.isEmpty

  private[transactionMetadataService] final def tryCompleteProducerNotify: ProducerTransactionRecord => Unit => Unit = {producerTransactionRecord => _ =>
    scala.util.Try(getStreamObjByID(producerTransactionRecord.stream)) match {
      case scala.util.Success(stream) =>
        producerNotifies.values().forEach(notify =>
          if (notify.notifyOn(
            ProducerTransaction(stream.name, producerTransactionRecord.partition, producerTransactionRecord.transactionID, producerTransactionRecord.state, producerTransactionRecord.quantity, producerTransactionRecord.ttl)
          )) notify.notificationPromise.trySuccess(value = Unit))
      case _ =>
    }
  }
}

private class ProducerTransactionNotification(val notifyOn: ProducerTransaction => Boolean,
                                              val notificationPromise: scala.concurrent.Promise[Unit])
