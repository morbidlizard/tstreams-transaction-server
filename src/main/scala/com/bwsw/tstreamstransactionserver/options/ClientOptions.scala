package com.bwsw.tstreamstransactionserver.options

object ClientOptions {
  /**
    *
    * @param connectionTimeoutMs
    * @param retryDelayMs
    * @param threadPool
    */
  case class ConnectionOptions(connectionTimeoutMs: Int = 5000, retryDelayMs: Int = 200, threadPool: Int = 4)
}
