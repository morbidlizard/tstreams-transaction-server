package transactionService

import java.nio.ByteBuffer
import java.util.concurrent.Executors

import com.twitter.util.{ExecutorServiceFuturePool, FuturePool}

class Context (threadNumber: Int) {
  require(threadNumber > 0)
  private def newExecutionContext: ExecutorServiceFuturePool = FuturePool.interruptible(Executors.newSingleThreadExecutor())
  val contexts = (1 to threadNumber).map(_=> newExecutionContext).toArray
  def getContext(value: Long): ExecutorServiceFuturePool = contexts((value % threadNumber).toInt)
  def getContext(stream: Int, partition: Int): ExecutorServiceFuturePool = getContext(ByteBuffer.allocate(8).putInt(stream).putInt(partition).getLong(0))
}

object Context {
  def apply(threadNumber: Int): Context = new Context(threadNumber)
  lazy val transactionContexts = Context(configProperties.ThreadPool.TransactionMetaServiceThreadPoolNumber)
  final lazy val transactionDataContext = Context(1)
}
