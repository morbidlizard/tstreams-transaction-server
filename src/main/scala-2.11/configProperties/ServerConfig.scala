package configProperties

import configProperties.Config._

object ServerConfig {
  val config = new Config("src/main/resources/serverProperties.properties")

  val transactionServerAddress = config.readProperty[String]("transactionServer.address")

  val transactionServerEndpoints = config.readProperty[String]("transactionServer.replication.endpoints")

  val transactionServerReplicationName = config.readProperty[String]("transactionServer.replication.name")

  val transactionServerReplicationGroup = config.readProperty[String]("transactionServer.replication.group")

  val authTokenTimeExpiration = config.readProperty[Long]("auth.token.time.expiration")

  val authTokenActiveMax = config.readProperty[Int]("auth.token.active.max")

  val zkEndpoints = config.readProperty[String]("zk.endpoints")

  val zkTimeoutSession = config.readProperty[Int]("zk.timeout.session")

  val zkTimeoutConnection = config.readProperty[Int]("zk.timeout.connection")

  val zkTimeoutBetweenRetries = config.readProperty[Int]("zk.timeout.betweenRetries")

  val zkRetriesMax = config.readProperty[Int]("zk.retries.max")

  val zkPrefix = config.readProperty[String]("zk.prefix")

  val transactionTimeoutCleanOpened = config.readProperty[Int]("transaction.timeout.clean.opened(sec)")

  val transactionDataCleanAmount = config.readProperty[Int]("transaction.data.clean.amount")

  val transactionDataTtlAdd = config.readProperty[Int]("transaction.data.ttl.add")

  val transactionMetadataTtlAdd = config.readProperty[Int]("transaction.metadata.ttl.add")

  val dbPath = config.readProperty[String]("db.path")

  val berkeleyDBJEproperties = config.getAllProperties("je.")
}
