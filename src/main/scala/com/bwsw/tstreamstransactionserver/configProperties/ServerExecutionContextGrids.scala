package com.bwsw.tstreamstransactionserver.configProperties

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid

class ServerExecutionContextGrids(rocksWriteNThreads: Int, rocksReadNThreads: Int) {
  private val commitLogExecutionContextGrid = ExecutionContextGrid("CommitLogExecutionContextGrid-%d")
  private val serverWriteExecutionContextGrid = ExecutionContextGrid(rocksWriteNThreads, "ServerWriteExecutionContextGrid-%d")
  private val serverReadExecutionContextGrid = ExecutionContextGrid(rocksReadNThreads, "ServerReadExecutionContextGrid-%d")

  private val contextGrids = Seq(commitLogExecutionContextGrid,
    serverReadExecutionContextGrid, serverWriteExecutionContextGrid)

  val commitLogContext = commitLogExecutionContextGrid.getContext
  val serverWriteContext = serverWriteExecutionContextGrid.getContext
  val serverReadContext = serverReadExecutionContextGrid.getContext

  def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    contextGrids foreach (context => context.stopAccessNewTasks())
    contextGrids foreach (context => context.awaitAllCurrentTasksAreCompleted())
  }
}
