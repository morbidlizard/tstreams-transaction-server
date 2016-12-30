package netty

import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.ExecutionContext


class Context(threadNumber: Int) {
  require(threadNumber > 0)

  private def newExecutionContext = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Context-%d").build()))

  val contexts = Array.fill(threadNumber)(newExecutionContext)

  def getContext(value: Long) = contexts((value % threadNumber).toInt)

  val getContext = contexts(0)
}


object Context {
  def apply(threadNumber: Int): Context = new Context(threadNumber)
  val producerTransactionsContext = Context(1)
}



