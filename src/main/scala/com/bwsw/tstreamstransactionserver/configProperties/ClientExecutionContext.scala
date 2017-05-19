package com.bwsw.tstreamstransactionserver.configProperties
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.bwsw.tstreamstransactionserver.netty.ExecutionContext

class ClientExecutionContext(nThreads: Int) {
  private lazy val executionContext = ExecutionContext(
    nThreads, "ClientPool-%d"
  )

  lazy val context = executionContext.getContext

  def stopAccessNewTasks(): Unit = executionContext.stopAccessNewTasks()
  def awaitAllCurrentTasksAreCompleted(): Unit = executionContext.awaitAllCurrentTasksAreCompleted()
  def stopAccessNewTasksAndAwaitCurrentTasksToBeCompleted(): Unit = {
    stopAccessNewTasks()
    awaitAllCurrentTasksAreCompleted()
  }
}
