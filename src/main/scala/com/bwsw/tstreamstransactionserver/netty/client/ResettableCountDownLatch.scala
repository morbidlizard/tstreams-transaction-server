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
package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.AbstractQueuedSynchronizer

class ResettableCountDownLatch(val count: Int) {

  if (count < 0)
    throw new IllegalArgumentException("count < 0")

  private final class Sync(val count: Int) extends AbstractQueuedSynchronizer {
    var startCount = count
    setState(startCount)

    def getCount = getState

    override def tryAcquireShared(acquires: Int): Int = if (getState == 0) 1 else -1

    override def tryReleaseShared(releases: Int): Boolean = {
      while (true) {
        val c: Int = getState
        if (c == 0)
          return false
        val nextc: Int = c - 1
        if (compareAndSetState(c, nextc))
          return nextc == 0
      }
      false
    }

    def setValue(value: Int) = setState(value)

    def reset() = setState(startCount)
  }

  private val sync: Sync = new Sync(count)

  override def toString = super.toString + "[Count = " + sync.getCount + "]"

  def reset(): Unit = sync.reset()

  def setValue(value: Int) = sync.setValue(value)

  def countDown(): Unit = sync.releaseShared(1)

  def getCount = sync.getCount

  @throws[InterruptedException]
  def await(timeout: Long, unit: TimeUnit): Boolean =
    sync.tryAcquireSharedNanos(1, unit.toNanos(timeout))

  @throws[InterruptedException]
  def await(): Unit =
    sync.acquireSharedInterruptibly(1)

}
