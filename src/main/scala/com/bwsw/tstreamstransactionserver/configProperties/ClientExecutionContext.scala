package com.bwsw.tstreamstransactionserver.configProperties
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.bwsw.tstreamstransactionserver.netty.ExecutionContext

class ClientExecutionContext(nThreads: Int) {
  private lazy val executionContext = ExecutionContext(
    Executors.newFixedThreadPool(nThreads, new ThreadFactoryBuilder().setNameFormat("ClientPool-%d").build())
  )

  lazy val context = executionContext.getContext

  def shutdown() = {
    executionContext.shutdown()
  }
}
