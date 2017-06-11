package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid

import scala.concurrent.ExecutionContextExecutorService

final class OrderedExecutionContextPool(poolNumber: Int) {
  private val singleThreadPools =
    Array.fill(poolNumber)(ExecutionContextGrid.apply("OrderedExecutionService-%d"))

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
