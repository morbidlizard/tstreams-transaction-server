package com.bwsw.tstreamstransactionserver.options

object ClientOptions {
  /** The options are applied as filters on establishing connection to a server.
    *
    * @param connectionTimeoutMs the time to wait while trying to establish a connection to a server.
    * @param retryDelayMs        delays between retry attempts.
    * @param requestTimeoutMs    the time to wait a request is completed and response is accepted. On setting option also take into consideration [[com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions]].
    * @param threadPool          the number of threads of thread pool to serialize/deserialize requests/responses.
    */
  case class ConnectionOptions(connectionTimeoutMs: Int = 5000, requestTimeoutMs: Int = 500,
                               requestTimeoutRetryCount: Int = 3, retryDelayMs: Int = 200, threadPool: Int = 2)

  /** The options are used to validate client requests by a server.
    *
    * @param key the key to authorize.
    */
  case class AuthOptions(key: String = "") extends AnyVal
}
