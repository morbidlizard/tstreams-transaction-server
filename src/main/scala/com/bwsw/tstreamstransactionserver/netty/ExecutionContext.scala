package com.bwsw.tstreamstransactionserver.netty

import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy
import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

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

  def awaitAllCurrentTasksAreCompleted(): Unit = contexts.foreach(_.awaitTermination(ExecutionContext.TasksCompletedTLL, TimeUnit.MILLISECONDS))
}


object ExecutionContext {
  /** The time to wait all tasks completed by thread pool */
  private val TasksCompletedTLL = 10000

  /** Creates an 1 single-threaded context with name */
  def apply(nameFormat: String) = new ExecutionContext(
    1,
    new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(), new ThreadFactoryBuilder().setNameFormat(nameFormat).build(), new DiscardPolicy())
  )
  /** Creates FixedThreadPool with defined threadNumber*/
  def apply(threadNumber: Int, nameFormat: String) = new ExecutionContext(
    1,
    new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue(), new ThreadFactoryBuilder().setNameFormat(nameFormat).build(), new DiscardPolicy())
  )
}

