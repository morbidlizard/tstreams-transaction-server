package com.bwsw.tstreamstransactionserver.options


object CommonOptions {
  val propertyFileName = "config"

  /** The options are applied on establishing connection to a ZooKeeper server(cluster).
    *
    * @param endpoints the socket address(es) of ZooKeeper servers.
    * @param prefix the coordination path to get/put socket address of t-streams transaction server.
    * @param sessionTimeoutMs the time to wait while trying to re-establish a connection to a ZooKeepers server(s).
    * @param retryDelayMs delays between retry attempts to establish connection to ZooKeepers server on case of lost connection.
    * @param connectionTimeoutMs the time to wait while trying to establish a connection to a ZooKeepers server(s) on first connection.
    */
  case class ZookeeperOptions(endpoints: String = "127.0.0.1:37001", prefix: String = "/tts",
                              sessionTimeoutMs: Int = 10000, retryDelayMs: Int = 500, connectionTimeoutMs: Int = 10000)

}
