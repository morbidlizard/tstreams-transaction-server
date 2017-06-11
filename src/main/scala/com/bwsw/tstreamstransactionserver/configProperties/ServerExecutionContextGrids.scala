package com.bwsw.tstreamstransactionserver.configProperties

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid

import scala.concurrent.ExecutionContextExecutorService

class ServerExecutionContextGrids(rocksWriteNThreads: Int, rocksReadNThreads: Int) {
  private val commitLogExecutionContext = ExecutionContextGrid("CommitLogPool-%d")
  private val serverWriteExecutionContext = ExecutionContextGrid(rocksWriteNThreads, "ServerWritePool-%d")
  private val serverReadExecutionContext = ExecutionContextGrid(rocksReadNThreads, "ServerReadPool-%d")

  val commitLogContext: ExecutionContextExecutorService = commitLogExecutionContext.getContext
  val serverWriteContext: ExecutionContextExecutorService = serverWriteExecutionContext.getContext
  val serverReadContext: ExecutionContextExecutorService = serverReadExecutionContext.getContext

  def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    val contexts = collection.immutable.Seq(
      commitLogExecutionContext,
      serverReadExecutionContext,
      serverWriteExecutionContext
    )
    contexts foreach (context => context.stopAccessNewTasks())
    contexts foreach (context => context.awaitAllCurrentTasksAreCompleted())
  }
}
