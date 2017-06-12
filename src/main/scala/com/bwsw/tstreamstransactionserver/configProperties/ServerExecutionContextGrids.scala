package com.bwsw.tstreamstransactionserver.configProperties

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid

class ServerExecutionContextGrids(rocksWriteNThreads: Int, rocksReadNThreads: Int) {
  private val commitLogExecutionContext = ExecutionContextGrid("CommitLogPool-%d")
  private val serverWriteExecutionContext = ExecutionContextGrid(rocksWriteNThreads, "ServerWritePool-%d")
  private val serverReadExecutionContext = ExecutionContextGrid(rocksReadNThreads, "ServerReadPool-%d")

  val commitLogContext = commitLogExecutionContext.getContext
  val serverWriteContext = serverWriteExecutionContext.getContext
  val serverReadContext = serverReadExecutionContext.getContext

  def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    val contextGrids = collection.immutable.Seq(
      commitLogExecutionContext,
      serverReadExecutionContext,
      serverWriteExecutionContext
    )
    contextGrids foreach (context => context.stopAccessNewTasks())
    contextGrids foreach (context => context.awaitAllCurrentTasksAreCompleted())
  }
}
