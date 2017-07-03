package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.ExecutionContext

import scala.concurrent.ExecutionContextExecutorService

final class OrderedExecutionPool(poolNumber: Int) {
  private val singleThreadPools =
    Array.fill(poolNumber)(ExecutionContext.apply("OrderedExecutionService-%d"))

  def pool(stream: Int, partition: Int): ExecutionContextExecutorService = {
    val n = singleThreadPools.length
    singleThreadPools(((stream % n) + (partition % n)) % n)
      .getContext
  }

  def close(): Unit = {
    singleThreadPools.foreach{pool =>
      pool.stopAccessNewTasks()
      pool.awaitAllCurrentTasksAreCompleted()
    }
  }
}
