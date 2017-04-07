package com.bwsw.tstreamstransactionserver.netty

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

/** Context is a wrapper for java executors
  *
  *  @constructor creates a context with a number of executors services.
  *  @param nContexts a number of executors services
  *  @param f an executor service.
  *
  */
class ExecutionContext(nContexts: Int, f: => java.util.concurrent.ExecutorService) {
  require(nContexts > 0)

  private def newExecutionContext = scala.concurrent.ExecutionContext.fromExecutorService(f)

  private val contexts = Array.fill(nContexts)(newExecutionContext)

  def getContext(value: Long) = contexts((value % nContexts).toInt)

  lazy val getContext = contexts(0)

  def stopAccessNewTasks(): Unit = contexts.foreach(_.shutdown())

  def awaitAllCurrentTasksAreCompleted(): Unit = contexts.foreach(_.awaitTermination(10000, TimeUnit.MILLISECONDS))
}


object ExecutionContext {
  def apply(contextNum: Int, f: => java.util.concurrent.ExecutorService): ExecutionContext = new ExecutionContext(contextNum, f)
  /** Creates a number of single-threaded contexts with names */
  def apply(contextNum: Int, nameFormat: String) = new ExecutionContext(contextNum, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(nameFormat).build()))
  /** Creates a context with 1 pool of any executor service*/
  def apply(f: => java.util.concurrent.ExecutorService) = new ExecutionContext(1, f)
}

