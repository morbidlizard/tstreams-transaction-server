package com.bwsw.tstreamstransactionserver.netty

import java.util.concurrent.{ExecutorService, Executors}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.ExecutionContext


/** Context is a wrapper for java executors
  *
  *  @constructor creates a context with a number of executors services.
  *  @param contextNum a number of executors services
  *  @param f an executor service.
  *
  */
class Context(contextNum: Int, f: => ExecutorService) {
  require(contextNum > 0)

  private val executorServices = Array.fill(contextNum)(f)

  private def newExecutionContext(executorService: ExecutorService) = ExecutionContext.fromExecutor(executorService)

  private val contexts = executorServices.map(newExecutionContext)

  def getContext(value: Long) = contexts((value % contextNum).toInt)

  lazy val getContext = contexts(0)

  def shutdown() = {
    executorServices.foreach(_.shutdown())
  }
}


object Context {
  def apply(contextNum: Int, f: => ExecutorService): Context = new Context(contextNum, f)
  /** Creates a number of single-threaded contexts with names */
  def apply(contextNum: Int, nameFormat: String) = new Context(contextNum, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(nameFormat).build()))
  /** Creates a context with 1 pool of any executor service*/
  def apply(f: => ExecutorService) = new Context(1, f)
}

