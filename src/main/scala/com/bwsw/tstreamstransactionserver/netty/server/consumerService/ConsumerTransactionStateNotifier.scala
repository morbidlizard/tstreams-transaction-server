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
package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction

import scala.concurrent.ExecutionContext

trait ConsumerTransactionStateNotifier {
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

  private[consumerService] final def tryCompleteConsumerNotify: ConsumerTransactionRecord => Unit => Unit = {
    consumerTransactionRecord =>
      _ =>
        consumerNotifies.values().forEach(notify =>
          if (notify.notifyOn(consumerTransactionRecord)) notify.notificationPromise.trySuccess(value = Unit))
  }
}

private class ConsumerTransactionNotification(val notifyOn: ConsumerTransaction => Boolean,
                                              val notificationPromise: scala.concurrent.Promise[Unit])

