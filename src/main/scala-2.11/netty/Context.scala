package netty

import java.util.concurrent.{ExecutorService, Executors}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.ExecutionContext


class Context(contextNum: Int, f: => ExecutorService) {
  require(contextNum > 0)

  private def newExecutionContext = ExecutionContext.fromExecutor(f)

  val contexts = Array.fill(contextNum)(newExecutionContext)

  def getContext(value: Long) = contexts((value % contextNum).toInt)

  lazy val getContext = contexts(0)
}


object Context {
  def apply(contextNum: Int, f: => ExecutorService): Context = new Context(contextNum, f)
  def apply(contextNum: Int, nameFormat: String) = new Context(contextNum, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(nameFormat).build()))
  def apply(f: => ExecutorService) = new Context(1, f)
}



