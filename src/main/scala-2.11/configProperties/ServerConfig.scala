package configProperties

import configProperties.Config._

object ServerConfig {

  private val config = new Config("src/main/resources/serverProperties.properties")

  val transactionServerAddress = config.readProperty[String]("transactionServer.address")

  val transactionServerEndpoints = config.readProperty[String]("transactionServer.replication.endpoints")

  val transactionServerReplicationName = config.readProperty[String]("transactionServer.replication.name")

  val transactionServerReplicationGroup = config.readProperty[String]("transactionServer.replication.group")

  val zkEndpoints = config.readProperty[String]("zk.endpoints", ',')

  val zkTimeoutSession = config.readProperty[Int]("zk.timeout.session")

  val zkTimeoutConnection = config.readProperty[Int]("zk.timeout.connection")

  val zkTimeoutBetweenRetries = config.readProperty[Int]("zk.timeout.betweenRetries")

  val zkRetriesMax = config.readProperty[Int]("zk.retries.max")

  val zkPrefix = config.readProperty[String]("zk.prefix")

  val authAddress = config.readProperty[String]("auth.address")

  val authTimeoutConnection = config.readProperty[Int]("auth.timeout.connection")

  val authTimeoutExponentialBetweenRetries = config.readProperty[Int]("auth.timeout.exponentialBetweenRetries")

  val transactionDataTtlAdd = config.readProperty[Int]("transaction.data.ttl.add")
}
