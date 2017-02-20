package com.bwsw.tstreamstransactionserver.options


object CommonOptions {
  /**
    *
    * @param endpoints
    * @param prefix
    * @param sessionTimeoutMs
    * @param retryCount
    * @param retryDelayMs
    * @param connectionTimeoutMs
    */
  case class ZookeeperOptions(endpoints: String = "127.0.0.1:2181", prefix: String = "/tts", sessionTimeoutMs: Int = 10000,
                              retryCount: Int = 5, retryDelayMs: Int = 500, connectionTimeoutMs: Int = 10000)

  /**
    *
    * @param key
    * @param connectionTimeoutMs
    * @param retryDelayMs
    * @param tokenRetryDelayMs
    * @param tokenConnectionTimeoutMs
    */
  case class AuthOptions(key: String = "", connectionTimeoutMs: Int = 5000, retryDelayMs: Int = 500,
                         tokenRetryDelayMs: Int = 200, tokenConnectionTimeoutMs: Int = 5000, ttl: Int = 120, cacheSize: Int = 100)
}
