package com.bwsw.tstreamstransactionserver.configProperties

import com.bwsw.tstreamstransactionserver.netty.ExecutionContext

import scala.concurrent.ExecutionContextExecutorService

class ServerExecutionContext(rocksWriteNThreads: Int, rocksReadNThreads: Int) {
  private val commitLogExecutionContext = ExecutionContext("CommitLogPool-%d")
  private val serverWriteExecutionContext = ExecutionContext(rocksWriteNThreads, "ServerWritePool-%d")
  private val serverReadExecutionContext = ExecutionContext(rocksReadNThreads, "ServerReadPool-%d")

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
