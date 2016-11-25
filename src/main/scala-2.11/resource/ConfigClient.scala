package resource

import java.io.FileInputStream
import java.util.Properties

class ConfigClient(pathToConfig: String) {
  private lazy val properties = {
    val file = new Properties()
    val in   = new FileInputStream(pathToConfig)
    file.load(in)
    in.close()
    file
  }

  // coordination.hosts = 192.1.1.1:2181,192.1.1.2:2181,192.1.1.3:2181
  // coordination.prefix = /ab/c/d/ -> master {"1.2.3.4", 8888} - ephemeral node
  // coordination.session.timeout_ms = 5000

  // listen.address = "1.2.3.4"
  // listen.port = 8888

  val login = Option(properties.getProperty("auth.login"))
    .getOrElse(throw new NoSuchElementException("auth.login isn't defined"))

  val password = Option(properties.getProperty("auth.password"))
    .getOrElse(throw new NoSuchElementException("auth.password isn't defined"))

  val zkEndpoints = Option(properties.getProperty("zk.endpoints"))
    .getOrElse(throw new NoSuchElementException("zk.address isn't defined"))
    .split(',')

  val zkTimeoutSession = Option(properties.getProperty("zk.timeout.session"))
    .getOrElse(throw new NoSuchElementException("zk.timeout.session isn't defined"))
    .toInt

  val zkTimeoutConnection = Option(properties.getProperty("zk.timeout.connection"))
    .getOrElse(throw new NoSuchElementException("zk.timeout.session isn't defined"))
    .toInt

  val zkTimeoutBetweenRetries = Option(properties.getProperty("zk.timeout.betweenRetries"))
    .getOrElse(throw new NoSuchElementException("zk.timeout.betweenRetries isn't defined"))
    .toInt

  val zkRetriesMax = Option(properties.getProperty("zk.retries.max"))
    .getOrElse(throw new NoSuchElementException("zk.retries.max isn't defined"))
    .toInt

  val zkPrefix = Option(properties.getProperty("zk.prefix"))
    .getOrElse(throw new NoSuchElementException("zk.prefix isn't defined"))

  val authAddress = Option(properties.getProperty("auth.address"))
    .getOrElse(throw new NoSuchElementException("auth.address isn't defined"))

  val authTimeoutConnection = Option(properties.getProperty("auth.timeout.connection"))
    .getOrElse(throw new NoSuchElementException("auth.timeout.connection isn't defined"))
    .toInt

  val authTimeoutExponentialBetweenRetries =  Option(properties.getProperty("auth.timeout.exponentialBetweenRetries"))
    .getOrElse(throw new NoSuchElementException("auth.timeout.exponentialBetweenRetries isn't defined"))
    .toInt


}
