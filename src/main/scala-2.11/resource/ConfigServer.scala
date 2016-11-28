package resource


class ConfigServer(pathToConfig: String) extends Config(pathToConfig) {
  import Config.stringToSting

  val transactionServerEndpoints = readProperty("transactionServer.endpoints", ',')

  val zkEndpoints = readProperty("zk.endpoints", ',')

  val zkTimeoutSession = readProperty("zk.timeout.session")(Config.stringToInt)

  val zkTimeoutConnection = readProperty("zk.timeout.connection")(Config.stringToInt)

  val zkTimeoutBetweenRetries = readProperty("zk.timeout.betweenRetries")(Config.stringToInt)

  val zkRetriesMax = readProperty("zk.retries.max")(Config.stringToInt)

  val zkPrefix = readProperty("zk.prefix")

  val authAddress = readProperty("auth.address")

  val authTimeoutConnection = readProperty("auth.timeout.connection")(Config.stringToInt)

  val authTimeoutExponentialBetweenRetries = readProperty("auth.timeout.exponentialBetweenRetries")(Config.stringToInt)
}
