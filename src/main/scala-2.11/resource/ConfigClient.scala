package resource

class ConfigClient(pathToConfig: String) extends Config(pathToConfig) {
  import Config.stringToSting
  // coordination.hosts = 192.1.1.1:2181,192.1.1.2:2181,192.1.1.3:2181
  // coordination.prefix = /ab/c/d/ -> master {"1.2.3.4", 8888} - ephemeral node
  // coordination.session.timeout_ms = 5000

  // listen.address = "1.2.3.4"
  // listen.port = 8888


  val login = readProperty("auth.login")

  val password = readProperty("auth.password")

  val zkEndpoints = readProperty("zk.endpoints", ',')

  val zkTimeoutSession = readProperty("zk.timeout.session")(Config.stringToInt)

  val zkTimeoutConnection = readProperty("zk.timeout.connection")(Config.stringToInt)

  val zkTimeoutBetweenRetries = readProperty("zk.timeout.betweenRetries")(Config.stringToInt)

  val zkRetriesMax = readProperty("zk.retries.max")(Config.stringToInt)

  val zkPrefix = readProperty("zk.prefix")

  val authAddress = readProperty("auth.address")

  val authTimeoutConnection = readProperty("auth.timeout.connection")(Config.stringToInt)

  val authTimeoutExponentialBetweenRetries =  readProperty("auth.timeout.exponentialBetweenRetries")(Config.stringToInt)
}
