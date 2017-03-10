package com.bwsw.tstreamstransactionserver.options

object ClientOptions {
  /**
    *
    * @param connectionTimeoutMs
    * @param retryDelayMs
    * @param threadPool
    */
  case class ConnectionOptions(connectionTimeoutMs: Int = 5000, requestTimeoutMs: Int = 1000,
                               requestTimeoutRetryCount: Int = 3, retryDelayMs: Int = 200, threadPool: Int = 4)

  case class AuthOptions(key: String = "") extends AnyVal
}
