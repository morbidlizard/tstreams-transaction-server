package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.{StreamCache, Time}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.BootstrapOptions
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction

import scala.concurrent.ExecutionContext

trait ProducerTransactionStateNotifier extends StreamCache {
  private implicit lazy val notifierContext: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
  private val notifies = new java.util.concurrent.ConcurrentHashMap[Long, ProducerTransactionNotification](0)
  private lazy val seq = new AtomicLong(0L)

  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean, func: => Unit): Long = {
    val producerNotification = new ProducerTransactionNotification(onNotificationCompleted, scala.concurrent.Promise[Unit]())
    val id = seq.getAndIncrement()

    notifies.put(id, producerNotification)

    producerNotification.notificationPromise.future.map{ onCompleteSuccessfully =>
      notifies.remove(id)
      func
    }
    id
  }

  private def removeNotify(id :Long) = notifies.remove(id) != null

  private[transactionMetadataService] def areThereAnyNotifies = !notifies.isEmpty

  private[transactionMetadataService] final def tryCompleteNotify(producerTransactionKey: ProducerTransactionKey): Unit = {
    scala.util.Try(getStreamObjByID(producerTransactionKey.stream)) match {
      case scala.util.Success(stream) =>
        notifies.values().forEach(notify =>
          if (notify.notifyOn(
            ProducerTransaction(stream.name, producerTransactionKey.partition, producerTransactionKey.transactionID, producerTransactionKey.state, producerTransactionKey.quantity, producerTransactionKey.ttl)
          )) notify.notificationPromise.trySuccess(value = Unit))
      case _ =>
    }
  }
}

class ProducerTransactionNotification(val notifyOn: ProducerTransaction => Boolean,
                                      val notificationPromise: scala.concurrent.Promise[Unit])
