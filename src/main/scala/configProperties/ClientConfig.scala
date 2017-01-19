package configProperties
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import configProperties.Config._
import netty.Context

class ClientConfig(config: Config) extends Config {
  override val properties: Map[String, String] = config.properties

  val clientPool = config.readProperty[Int]("client.pool")

  val serverTimeoutConnection = config.readProperty[Int]("server.timeout.connection")

  val serverTimeoutBetweenRetries = config.readProperty[Int]("server.timeout.betweenRetries")

  val login = config.readProperty[String]("auth.login")

  val password = config.readProperty[String]("auth.password")

  val zkEndpoints = config.readProperty[String]("zk.endpoints")

  val zkTimeoutSession = config.readProperty[Int]("zk.timeout.session")

  val zkTimeoutConnection = config.readProperty[Int]("zk.timeout.connection")

  val zkTimeoutBetweenRetries = config.readProperty[Int]("zk.timeout.betweenRetries")

  val zkRetriesMax = config.readProperty[Int]("zk.retries.max")

  val zkPrefix = config.readProperty[String]("zk.prefix")

  val authTimeoutConnection = config.readProperty[Int]("auth.timeout.connection")

  val authTimeoutBetweenRetries = config.readProperty[Int]("auth.timeout.betweenRetries")

  val authTokenTimeoutConnection = config.readProperty[Int]("auth.token.timeout.connection")

  val authTokenTimeoutBetweenRetries = config.readProperty[Int]("auth.token.timeout.betweenRetries")

  val clientPoolContext = Context(Executors.newFixedThreadPool(clientPool,
    new ThreadFactoryBuilder().setNameFormat("ClientPool-%d").build())
  ).getContext
}
