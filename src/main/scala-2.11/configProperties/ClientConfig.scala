package configProperties
import configProperties.Config._
object ClientConfig {
  // coordination.hosts = 192.1.1.1:2181,192.1.1.2:2181,192.1.1.3:2181
  // coordination.prefix = /ab/c/d/ -> master {"1.2.3.4", 8888} - ephemeral node
  // coordination.session.timeout_ms = 5000

  // listen.address = "1.2.3.4"
  // listen.port = 8888

  private val config = new Config("src/main/resources/clientProperties.properties")

  val login = config.readProperty[String]("auth.login")

  val password = config.readProperty[String]("auth.password")

  val zkEndpoints = config.readProperty[String]("zk.endpoints", ',')

  val zkTimeoutSession = config.readProperty[Int]("zk.timeout.session")

  val zkTimeoutConnection = config.readProperty[Int]("zk.timeout.connection")

  val zkTimeoutBetweenRetries = config.readProperty[Int]("zk.timeout.betweenRetries")

  val zkRetriesMax = config.readProperty[Int]("zk.retries.max")

  val zkPrefix = config.readProperty[String]("zk.prefix")

  val authAddress = config.readProperty[String]("auth.address")

  val authTimeoutConnection = config.readProperty[Int]("auth.timeout.connection")

  val authTimeoutBetweenRetries = config.readProperty[Int]("auth.timeout.betweenRetries")

  val authTokenTimeoutConnection = config.readProperty[Int]("auth.token.timeout.connection")

  val authTokenTimeoutBetweenRetries = config.readProperty[Int]("auth.token.timeout.betweenRetries")
}
