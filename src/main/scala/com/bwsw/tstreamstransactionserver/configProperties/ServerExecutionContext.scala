package com.bwsw.tstreamstransactionserver.configProperties

import com.bwsw.tstreamstransactionserver.netty.ExecutionContext

class ServerExecutionContext(nThreads: Int, berkeleyReadNThreads: Int, rocksWriteNThreads: Int, rocksReadNThreads: Int) {
  private val commitLogExecutionContext = ExecutionContext("CommitLogPool-%d")
  private val berkeleyWriteExecutionContext = ExecutionContext("BerkeleyWritePool-%d")
  private val berkeleyReadExecutionContext = ExecutionContext(berkeleyReadNThreads,"BerkeleyReadPool-%d")
  private val rocksWriteExecutionContext = ExecutionContext(rocksWriteNThreads, "RocksWritePool-%d")
  private val rocksReadExecutionContext = ExecutionContext(rocksReadNThreads, "RocksReadPool-%d")
  private val serverExecutionContext = ExecutionContext(nThreads, "ServerPool-%d")

  val commitLogContext = commitLogExecutionContext.getContext
  val berkeleyWriteContext = berkeleyWriteExecutionContext.getContext
  val berkeleyReadContext = berkeleyReadExecutionContext.getContext
  val rocksWriteContext = rocksWriteExecutionContext.getContext
  val rocksReadContext = rocksReadExecutionContext.getContext
  val context = serverExecutionContext.getContext

  def stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted(): Unit = {
    val contexts = collection.immutable.Seq(
      commitLogExecutionContext,
      berkeleyWriteExecutionContext,
      berkeleyReadExecutionContext,
      rocksWriteExecutionContext,
      rocksReadExecutionContext,
      serverExecutionContext
    )
    contexts foreach (context => context.stopAccessNewTasks())
    contexts foreach (context => context.awaitAllCurrentTasksAreCompleted())
  }
}
