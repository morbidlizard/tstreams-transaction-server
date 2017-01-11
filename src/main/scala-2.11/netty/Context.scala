package netty

import java.util.concurrent.{ExecutorService, Executors}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool


class Context(contextNum: Int, f: => ExecutorService) {
  require(contextNum > 0)

  private def newExecutionContext = ExecutionContext.fromExecutor(f)

  val contexts = Array.fill(contextNum)(newExecutionContext)

  def getContext(value: Long) = contexts((value % contextNum).toInt)

  val getContext = contexts(0)
}


object Context {

  def apply(contextNum: Int, f: => ExecutorService): Context = new Context(contextNum, f)
  def apply(contextNum: Int, nameFormat: String) = new Context(contextNum, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(nameFormat).build()))
  def apply(f: => ExecutorService) = new Context(1, f)

  def serverPool = Context(Executors.newFixedThreadPool(configProperties.ServerConfig.transactionServerPool, new ThreadFactoryBuilder().setNameFormat("ServerPool-%d").build()))
  lazy val berkeleyWritePool = Context(1, "BerkeleyWritePool-%d")
  lazy val berkeleyReadPool = Context(Executors.newFixedThreadPool(configProperties.ServerConfig.transactionServerBerkeleyReadPool, new ThreadFactoryBuilder().setNameFormat("BerkeleyReadPool-%d").build()))
  lazy val rocksWritePool = //Context(new ForkJoinPool(configProperties.ServerConfig.transactionServerRocksDBWritePool))
  Context(Executors.newFixedThreadPool(configProperties.ServerConfig.transactionServerRocksDBWritePool, new ThreadFactoryBuilder().setNameFormat("RocksWritePool-%d").build()))
  lazy val rocksReadPool = Context(Executors.newFixedThreadPool(configProperties.ServerConfig.transactionServerRocksDBReadPool, new ThreadFactoryBuilder().setNameFormat("RocksReadPool-%d").build()))
}



