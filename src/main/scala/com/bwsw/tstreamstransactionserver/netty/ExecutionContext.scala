package com.bwsw.tstreamstransactionserver.netty

import java.util.concurrent.{ExecutorService, Executors}

import com.google.common.util.concurrent.ThreadFactoryBuilder

/** Context is a wrapper for java executors
  *
  *  @constructor creates a context with a number of executors services.
  *  @param nContexts a number of executors services
  *  @param f an executor service.
  *
  */
class ExecutionContext(nContexts: Int, f: => ExecutorService) {
  require(nContexts > 0)

  private val executorServices = Array.fill(nContexts)(f)

  private def newExecutionContext(executorService: ExecutorService) = concurrent.ExecutionContext.fromExecutor(executorService)

  private val contexts = executorServices.map(newExecutionContext)

  def getContext(value: Long) = contexts((value % nContexts).toInt)

  lazy val getContext = contexts(0)

  def shutdown() = {
    executorServices.foreach(_.shutdown())
  }
}


object ExecutionContext {
  def apply(contextNum: Int, f: => ExecutorService): ExecutionContext = new ExecutionContext(contextNum, f)
  /** Creates a number of single-threaded contexts with names */
  def apply(contextNum: Int, nameFormat: String) = new ExecutionContext(contextNum, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(nameFormat).build()))
  /** Creates a context with 1 pool of any executor service*/
  def apply(f: => ExecutorService) = new ExecutionContext(1, f)
}

