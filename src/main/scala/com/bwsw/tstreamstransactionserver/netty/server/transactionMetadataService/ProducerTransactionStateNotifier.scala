/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction

import scala.concurrent.ExecutionContext

class ProducerTransactionStateNotifier {
  private implicit lazy val notifierProducerContext: ExecutionContext =
    scala.concurrent.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
  private val producerNotifies =
    new java.util.concurrent.ConcurrentHashMap[Long, ProducerTransactionNotification](0)
  private lazy val producerSeq =
    new AtomicLong(0L)


  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean,
                                               func: => Unit): Long = {
    val producerNotification = new ProducerTransactionNotification(onNotificationCompleted, scala.concurrent.Promise[Unit]())
    val id = producerSeq.getAndIncrement()

    producerNotifies.put(id, producerNotification)

    producerNotification.notificationPromise.future.map { onCompleteSuccessfully =>
      producerNotifies.remove(id)
      func
    }
    id
  }

  final def removeProducerTransactionNotification(id: Long): Boolean =
    producerNotifies.remove(id) != null

  private[transactionMetadataService] final def areThereAnyProducerNotifies =
    !producerNotifies.isEmpty

  private[transactionMetadataService] final def tryCompleteProducerNotify: ProducerTransactionRecord => Unit => Unit = { producerTransactionRecord =>
    _ =>
      producerNotifies.values().forEach(notify =>
        if (notify.notifyOn(producerTransactionRecord)) notify.notificationPromise.trySuccess(value = Unit))
  }
}

private class ProducerTransactionNotification(val notifyOn: ProducerTransaction => Boolean,
                                              val notificationPromise: scala.concurrent.Promise[Unit])
