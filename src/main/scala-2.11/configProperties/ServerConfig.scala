package configProperties

import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import configProperties.Config._
import netty.Context

class ServerConfig(config: Config) extends Config {
  override val properties: Map[String, String] = config.properties
//  val config = new ConfigFile("src/main/resources/serverProperties.properties")

  val transactionServerAddress = (System.getenv("HOST"), System.getenv("PORT0")) match {
    case (host, port) if host != null && port != null => s"$host:$port"
    case _ => s"${config.readProperty[String]("transactionServer.listen")}:${config.readProperty[String]("transactionServer.port")}"
  }

  val transactionServerListen = config.readProperty[String]("transactionServer.listen")

  val transactionServerPool = config.readProperty[Int]("transactionServer.pool")

//  val transactionServerBerkeleyWritePool = config.readProperty[Int]("transactionServer.berkeleyWritePool")

  val transactionServerBerkeleyReadPool = config.readProperty[Int]("transactionServer.berkeleyReadPool")

  val transactionServerRocksDBWritePool = config.readProperty[Int]("transactionServer.rocksDBWritePool")

  val transactionServerRocksDBReadPool = config.readProperty[Int]("transactionServer.rocksDBReadPool")

  val transactionServerPort =  config.readProperty[Int]("transactionServer.port")

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

  val dbStreamDirName   = config.readProperty[String]("db.path.stream")
  val streamStoreName = "StreamStore"

  val dbTransactionDataDirName = config.readProperty[String]("db.path.transaction_data")

  val dbTransactionMetaDirName   = config.readProperty[String]("db.path.transaction_meta")
  val transactionMetaStoreName = "TransactionStore"
  val transactionMetaOpenStoreName = "TransactionOpenStore"

  val consumerStoreName = "ConsumerStore"

  val berkeleyDBJEproperties = config.getAllProperties("je.")

  val options = new RocksDBConfig(config).rocksDBProperties

  val berkeleyWritePool = Context(1, "BerkeleyWritePool-%d")
  val berkeleyReadPool = Context(Executors.newFixedThreadPool(transactionServerBerkeleyReadPool, new ThreadFactoryBuilder().setNameFormat("BerkeleyReadPool-%d").build()))
  val rocksWritePool = Context(Executors.newFixedThreadPool(transactionServerRocksDBWritePool, new ThreadFactoryBuilder().setNameFormat("RocksWritePool-%d").build()))
  val rocksReadPool = Context(Executors.newFixedThreadPool(transactionServerRocksDBReadPool, new ThreadFactoryBuilder().setNameFormat("RocksReadPool-%d").build()))
  val transactionServerPoolContext = Context(Executors.newFixedThreadPool(transactionServerPool, new ThreadFactoryBuilder().setNameFormat("ServerPool-%d").build()))

}
