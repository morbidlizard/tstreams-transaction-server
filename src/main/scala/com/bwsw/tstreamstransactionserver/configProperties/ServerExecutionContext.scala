package com.bwsw.tstreamstransactionserver.configProperties

import java.util.concurrent.Executors

import com.bwsw.tstreamstransactionserver.netty.ExecutionContext
import com.google.common.util.concurrent.ThreadFactoryBuilder

class ServerExecutionContext(nThreads: Int, berkeleyReadNThreads: Int, rocksWriteNThreads: Int, rocksReadNThreads: Int) {
  private val berkeleyWriteExecutionContext = ExecutionContext(1, "BerkeleyWritePool-%d")
  private val berkeleyReadExecutionContext = ExecutionContext(Executors.newFixedThreadPool(berkeleyReadNThreads, new ThreadFactoryBuilder().setNameFormat("BerkeleyReadPool-%d").build()))
  private val rocksWriteExecutionContext = ExecutionContext(Executors.newFixedThreadPool(rocksWriteNThreads, new ThreadFactoryBuilder().setNameFormat("RocksWritePool-%d").build()))
  private val rocksReadExecutionContext = ExecutionContext(Executors.newFixedThreadPool(rocksReadNThreads, new ThreadFactoryBuilder().setNameFormat("RocksReadPool-%d").build()))
  private val serverExecutionContext = ExecutionContext(Executors.newFixedThreadPool(nThreads, new ThreadFactoryBuilder().setNameFormat("ServerPool-%d").build()))
  private val commitLogExecutionContext = ExecutionContext(1, "CommitLogPool-%d")

  val context = serverExecutionContext.getContext
  val berkeleyWriteContext = berkeleyWriteExecutionContext.getContext
  val berkeleyReadContext = berkeleyReadExecutionContext.getContext
  val rocksWriteContext = rocksWriteExecutionContext.getContext
  val rocksReadContext = rocksReadExecutionContext.getContext
  val commitLogContext = commitLogExecutionContext

  def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    val contexts = collection.immutable.Seq(
      berkeleyWriteExecutionContext,
      berkeleyReadExecutionContext,
      rocksWriteExecutionContext,
      rocksReadExecutionContext,
      serverExecutionContext,
      commitLogContext
    )
    contexts foreach (context => context.stopAccessNewTasks())
    contexts foreach (context => context.awaitAllCurrentTasksAreCompleted())
  }
}
