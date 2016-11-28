package resource


class ConfigServer(pathToConfig: String) extends Config(pathToConfig) {

  val transactionServerAddress = readProperty("transactionServer.address")(Config.stringToSting)

  val transactionServerEndpoints = readProperty("transactionServer.replication.endpoints")(Config.stringToSting)

  val transactionServerReplicationName = readProperty("transactionServer.replication.name")(Config.stringToSting)

  val transactionServerReplicationGroup = readProperty("transactionServer.replication.group")(Config.stringToSting)

  val zkEndpoints = readProperty("zk.endpoints", ',')(Config.stringToSting)

  val zkTimeoutSession = readProperty("zk.timeout.session")(Config.stringToInt)

  val zkTimeoutConnection = readProperty("zk.timeout.connection")(Config.stringToInt)

  val zkTimeoutBetweenRetries = readProperty("zk.timeout.betweenRetries")(Config.stringToInt)

  val zkRetriesMax = readProperty("zk.retries.max")(Config.stringToInt)

  val zkPrefix = readProperty("zk.prefix")(Config.stringToSting)

  val authAddress = readProperty("auth.address")(Config.stringToSting)

  val authTimeoutConnection = readProperty("auth.timeout.connection")(Config.stringToInt)

  val authTimeoutExponentialBetweenRetries = readProperty("auth.timeout.exponentialBetweenRetries")(Config.stringToInt)
}
