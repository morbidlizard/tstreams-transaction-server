package com.bwsw.tstreamstransactionserver.configProperties

import com.bwsw.tstreamstransactionserver.netty.ExecutionContext

class ServerExecutionContext(nThreads: Int, berkeleyReadNThreads: Int, rocksWriteNThreads: Int, rocksReadNThreads: Int) {
  private val berkeleyWriteExecutionContext = ExecutionContext("BerkeleyWritePool-%d")
  private val berkeleyReadExecutionContext = ExecutionContext(berkeleyReadNThreads,"BerkeleyReadPool-%d")
  private val rocksWriteExecutionContext = ExecutionContext(rocksWriteNThreads, "RocksWritePool-%d")
  private val rocksReadExecutionContext = ExecutionContext(rocksReadNThreads, "RocksReadPool-%d")
  private val commitLogExecutionContext = ExecutionContext("CommitLogPool-%d")
  private val serverExecutionContext = ExecutionContext(nThreads, "ServerPool-%d")

  val berkeleyWriteContext = berkeleyWriteExecutionContext.getContext
  val berkeleyReadContext = berkeleyReadExecutionContext.getContext
  val rocksWriteContext = rocksWriteExecutionContext.getContext
  val rocksReadContext = rocksReadExecutionContext.getContext
  val commitLogContext = commitLogExecutionContext.getContext
  val context = serverExecutionContext.getContext

  def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    val contexts = collection.immutable.Seq(
      berkeleyWriteExecutionContext,
      berkeleyReadExecutionContext,
      rocksWriteExecutionContext,
      rocksReadExecutionContext,
      commitLogExecutionContext,
      serverExecutionContext
    )
    contexts foreach (context => context.stopAccessNewTasks())
    contexts foreach (context => context.awaitAllCurrentTasksAreCompleted())
  }
}
