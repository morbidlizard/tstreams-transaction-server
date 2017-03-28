package com.bwsw.tstreamstransactionserver.configProperties

import java.util.concurrent.Executors

import com.bwsw.tstreamstransactionserver.netty.ExecutionContext
import com.google.common.util.concurrent.ThreadFactoryBuilder

class ServerExecutionContext(nThreads: Int, berkeleyReadNThreads: Int, rocksWriteNThreads: Int, rocksReadNThreads: Int) {
  private lazy val berkeleyWriteExecutionContext = ExecutionContext(1, "BerkeleyWritePool-%d")
  private lazy val berkeleyReadExecutionContext = ExecutionContext(Executors.newFixedThreadPool(berkeleyReadNThreads, new ThreadFactoryBuilder().setNameFormat("BerkeleyReadPool-%d").build()))
  private lazy val rocksWriteExecutionContext = ExecutionContext(Executors.newFixedThreadPool(rocksWriteNThreads, new ThreadFactoryBuilder().setNameFormat("RocksWritePool-%d").build()))
  private lazy val rocksReadExecutionContext = ExecutionContext(Executors.newFixedThreadPool(rocksReadNThreads, new ThreadFactoryBuilder().setNameFormat("RocksReadPool-%d").build()))
  private lazy val serverExecutionContext = ExecutionContext(Executors.newFixedThreadPool(nThreads, new ThreadFactoryBuilder().setNameFormat("ServerPool-%d").build()))

  lazy val context = serverExecutionContext.getContext
  lazy val berkeleyWriteContext = berkeleyWriteExecutionContext.getContext
  lazy val berkeleyReadContext = berkeleyReadExecutionContext.getContext
  lazy val rocksWriteContext = rocksWriteExecutionContext.getContext
  lazy val rocksReadContext = rocksReadExecutionContext.getContext

  def shutdown(): Unit = {
    berkeleyWriteExecutionContext.shutdown()
    berkeleyReadExecutionContext.shutdown()
    rocksWriteExecutionContext.shutdown()
    rocksReadExecutionContext.shutdown()
    serverExecutionContext.shutdown()
  }
}
