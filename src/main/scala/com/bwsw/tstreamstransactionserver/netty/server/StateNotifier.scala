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
package com.bwsw.tstreamstransactionserver.netty.server

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Promise}

private object StateNotifier {
  private implicit lazy val executionContext: ExecutionContext =
    scala.concurrent.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
}

final class StateNotifier[T] {

  import StateNotifier.executionContext

  private case class Notification(notifyOn: T => Boolean,
                                  notificationPromise: scala.concurrent.Promise[Unit])

  private val requestIdGenerator =
    new AtomicLong(0L)

  private val requests =
    scala.collection.concurrent.TrieMap.empty[Long, Notification]

  private val notifications =
    ArrayBuffer.empty[Promise[Unit]]

  def broadcastNotifications(): Unit = {
    notifications.foreach(notification =>
      notification.trySuccess(value = Unit)
    )
    notifications.clear()
  }


  def leaveRequest(onNotificationCompleted: T => Boolean,
                   func: => Unit): Long = {

    val producerNotification =
      Notification(
        onNotificationCompleted,
        scala.concurrent.Promise[Unit]()
      )

    val id = requestIdGenerator.getAndIncrement()

    requests.put(id, producerNotification)

    producerNotification
      .notificationPromise
      .future.map { _ => func }

    id
  }

  def removeRequest(id: Long): Boolean =
    requests.remove(id).isDefined

  def tryCompleteRequests(entity: T): Unit = {
    if (requests.nonEmpty) {
      requests
        .find { case (_, notify) =>
          notify.notifyOn(entity)
        }
        .foreach { case (id, notify) =>
          requests.remove(id, notify)
          notifications += notify.notificationPromise
        }
    }
  }
}
