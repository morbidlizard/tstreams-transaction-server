package com.bwsw.tstreamstransactionserver.configProperties

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid

class ClientExecutionContextGrid(nThreads: Int) {
  private lazy val contextGrid = ExecutionContextGrid(nThreads, "ClientPool-%d")

  lazy val context = contextGrid.getContext

  def stopAccessNewTasks(): Unit = contextGrid.stopAccessNewTasks()
  def awaitAllCurrentTasksAreCompleted(): Unit = contextGrid.awaitAllCurrentTasksAreCompleted()
  def stopAccessNewTasksAndAwaitCurrentTasksToBeCompleted(): Unit = {
    stopAccessNewTasks()
    awaitAllCurrentTasksAreCompleted()
  }
}
