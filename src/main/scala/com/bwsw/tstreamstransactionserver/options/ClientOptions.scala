package com.bwsw.tstreamstransactionserver.options

object ClientOptions {
  /** The options are applied as filters on establishing connection to a server
    *
    * @param connectionTimeoutMs the time to wait while trying to establish a connection to a server.
    * @param retryDelayMs delays between retry attempts.
    * @param requestTimeoutMs the time to wait a request is completed and response is accepted.
    * @param threadPool the number of threads of thread pool to serialize/deserialize requests/responses.
    */
  case class ConnectionOptions(connectionTimeoutMs: Int = 5000, requestTimeoutMs: Int = 500,
                               requestTimeoutRetryCount: Int = 3, retryDelayMs: Int = 200, threadPool: Int = 4)

  case class AuthOptions(key: String = "") extends AnyVal
}
