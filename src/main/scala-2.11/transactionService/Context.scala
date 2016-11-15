package transactionService

import java.nio.ByteBuffer
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class Context (threadNumber: Int) {
  require(threadNumber > 0)
  private def newExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  val contexts = (1 to threadNumber).map(_=> newExecutionContext).toArray
  def getContext: ExecutionContextExecutor = contexts.head
  def getContext(value: Long): ExecutionContextExecutor = contexts((value % threadNumber).toInt)
  def getContext(val1: Int, val2: Int): ExecutionContextExecutor = getContext(ByteBuffer.allocate(8).putInt(val1).putInt(val2).getLong(0))
}

object Context extends App {
  def apply(threadNumber: Int): Context = new Context(threadNumber)
  lazy val transactionContexts = Context(5)
}
