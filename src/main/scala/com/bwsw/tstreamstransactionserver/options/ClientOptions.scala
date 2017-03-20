package com.bwsw.tstreamstransactionserver.options

object ClientOptions {
  /**
    *
    * @param connectionTimeoutMs the time to wait while trying to establish a connection to a server.
    * @param retryDelayMs delays between retry attempts
    * @param requestTimeoutMs the time to wait while a request di.
    * @param threadPool the number of threads of thread pool to serialize/deserialize requests/responses.
    */
  case class ConnectionOptions(connectionTimeoutMs: Int = 5000, requestTimeoutMs: Int = 200,
                               requestTimeoutRetryCount: Int = 3, retryDelayMs: Int = 200, threadPool: Int = 4)

  case class AuthOptions(key: String = "") extends AnyVal
}
