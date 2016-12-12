package transactionService

import java.nio.ByteBuffer
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.twitter.util.{ExecutorServiceFuturePool, FuturePool}

class Context(threadNumber: Int) {
  require(threadNumber > 0)

  private def newExecutionContext: ExecutorServiceFuturePool = FuturePool.interruptible(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Context-%d").build()))

  val contexts = Array.fill(threadNumber)(newExecutionContext)

  def getContext(value: Long): ExecutorServiceFuturePool = contexts((value % threadNumber).toInt)

  def getContext(stream: Int, partition: Int): ExecutorServiceFuturePool = {
    def negativeIntToPositive(value: Int) = if (value < 0) -value else value

    getContext(ByteBuffer.allocate(8).putInt(negativeIntToPositive(stream)).putInt(negativeIntToPositive(partition)).getLong(0))
  }
}

object Context {
  def apply(threadNumber: Int): Context = new Context(threadNumber)
}
