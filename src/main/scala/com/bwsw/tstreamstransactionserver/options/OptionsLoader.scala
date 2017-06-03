package com.bwsw.tstreamstransactionserver.options

import java.io.FileInputStream
import java.util.Properties

import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import org.rocksdb.CompressionType


class OptionsLoader {
  require(System.getProperty(CommonOptions.propertyFileName) != null,
    s"There is no file with properties. " +
      s"You should define a path to a property file through '-D${CommonOptions.propertyFileName}=<path_to_file>' " +
      s"(e.g. 'java -D${CommonOptions.propertyFileName}=/home/user/config.properties " +
      s"-cp target/scala-2.12/tstreams-transaction-server-1.3.7.4-SNAPSHOT.jar:/home/user/slf4j-api-1.7.24.jar:/home/user/slf4j-simple-1.7.24.jar " +
      "com.bwsw.tstreamstransactionserver.ServerLauncher').")

  private val props = new Properties()
  props.load(new FileInputStream(System.getProperty(CommonOptions.propertyFileName)))

  private val serverAuthOptions = loadServerAuthOptions()
  private val zookeeperOptions = loadZookeeperOptions()
  private val bootstrapOptions = loadBootstrapOptions()
  private val serverReplicationOptions = loadServerReplicationOptions()
  private val serverStorageOptions = loadServerStorageOptions()
  private val serverRocksStorageOptions = loadServerRocksStorageOptions()
  private val packageTransmissionOptions = loadPackageTransmissionOptions()
  private val commitLogOptions = loadCommitLogOptions()
  private val zookeeperSpecificOptions = loadZookeeperSpecificOptions()
  private val subscribersUpdateOptions = loadSubscribersUpdateOptions()

  private lazy val helper = new OptionHelper(props)

  private def loadBootstrapOptions() = {
    implicit val typeTag = classOf[BootstrapOptions]

    val host =
      helper.castCheck("host", identity)
    val port =
      helper.castCheck("port", prop => prop.toInt)
    val orderedExecutionPoolSize =
      helper.castCheck("ordered.execution.pool.size", prop => prop.toInt)

    BootstrapOptions(
      host,
      port,
      orderedExecutionPoolSize
    )
  }

  private def loadSubscribersUpdateOptions() = {
    implicit val typeTag = classOf[SubscriberUpdateOptions]

    val updatePeriodMs =
      helper.castCheck("subscribers.update.period-ms", prop => prop.toInt)

    val subscriberMonitoringZkEndpoints =
      scala.util.Try(
        helper.castCheck("subscribers.monitoring.zk.endpoints", identity)
      ) match {
        case scala.util.Success(endpoints) => endpoints
        case scala.util.Failure(_) =>
          helper.castCheck("zk.endpoints", identity)
      }

    SubscriberUpdateOptions(
      updatePeriodMs,
      subscriberMonitoringZkEndpoints
    )
  }

  private def loadServerAuthOptions() = {
    implicit val typeTag = classOf[ServerOptions.AuthOptions]

    val key =
      helper.castCheck("key", identity)

    val activeTokensNumber =
      helper.castCheck("active.tokens.number", prop => prop.toInt)

    val tokenTTL =
      helper.castCheck("token.ttl", prop => prop.toInt)

    ServerOptions.AuthOptions(
      key,
      activeTokensNumber,
      tokenTTL
    )
  }

  private def loadServerStorageOptions() = {
    implicit val typeTag = classOf[StorageOptions]

    val path =
      helper.castCheck("path", identity)

    val streamZookeeperDirectory =
      helper.castCheck("stream.zookeeper.directory", identity)

    val dataDirectory =
      helper.castCheck("data.directory", identity)

    val metadataDirectory =
      helper.castCheck("metadata.directory", identity)

    val commitLogDirectory =
      helper.castCheck("commit.log.directory", identity)

    val commitLogRocksDirectory =
      helper.castCheck("commit.log.rocks.directory", identity)

    StorageOptions(
      path,
      streamZookeeperDirectory,
      dataDirectory,
      metadataDirectory,
      commitLogDirectory,
      commitLogRocksDirectory
    )
  }

  private def loadServerReplicationOptions() = {
    ServerReplicationOptions()
  }

  private def loadServerRocksStorageOptions() = {
    implicit val typeTag = classOf[RocksStorageOptions]

    val writeThreadPool =
      helper.castCheck("write.thread.pool", prop => prop.toInt)

    val readThreadPool =
      helper.castCheck("read.thread.pool",  prop => prop.toInt)

    val ttlAddMs =
      helper.castCheck("ttl.add-ms",  prop => prop.toInt)

    val transactionDatabaseTransactionKeeptimeMin =
      helper.castCheck("transaction-database.transaction-keeptime-min",  prop => prop.toInt)

    val transactionCacheSize =
      helper.castCheck("transaction.cache.size",  prop => prop.toInt)

    val maxBackgroundCompactions =
      helper.castCheck("max.background.compactions",  prop => prop.toInt)

    val compression =
      helper.castCheck("compression", prop => CompressionType.getCompressionType(prop))

    val useFsync =
      helper.castCheck("use.fsync", prop => prop.toBoolean)

    RocksStorageOptions(
      writeThreadPool,
      readThreadPool,
      ttlAddMs,
      transactionDatabaseTransactionKeeptimeMin,
      transactionCacheSize,
      maxBackgroundCompactions,
      compression,
      useFsync
    )
  }

  private def loadZookeeperOptions() = {
    implicit val typeTag = classOf[ZookeeperOptions]

    val endpoints =
      helper.castCheck("zk.endpoints", identity)

    val prefix =
      helper.castCheck("zk.prefix", identity)

    val sessionTimeoutMs =
      helper.castCheck("zk.session.timeout-ms", prop => prop.toInt)

    val retryDelayMs =
      helper.castCheck("zk.retry.delay-ms", prop => prop.toInt)

    val connectionTimeoutMs =
      helper.castCheck("zk.connection.timeout-ms", prop => prop.toInt)

    ZookeeperOptions(
      endpoints,
      prefix,
      sessionTimeoutMs,
      retryDelayMs,
      connectionTimeoutMs
    )
  }

  private def loadPackageTransmissionOptions() = {
    implicit val typeTag = classOf[TransportOptions]

    val maxMetadataPackageSize =
      helper.castCheck("max.metadata.package.size", prop => prop.toInt)

    val maxDataPackageSize =
      helper.castCheck("max.data.package.size", prop => prop.toInt)

    TransportOptions(
      maxMetadataPackageSize,
      maxDataPackageSize
    )
  }

  private def loadCommitLogOptions() = {
    implicit val typeTag = classOf[CommitLogOptions]

    val commitLogWriteSyncPolicy =
      helper.castCheck("commit.log.write.sync.policy", prop => CommitLogWriteSyncPolicy.withName(prop))

    val commitLogWriteSyncValue =
      helper.castCheck("commit.log.write.sync.value", prop => prop.toInt)

    val incompleteCommitLogReadPolicy =
      helper.castCheck("incomplete.commit.log.read.policy", prop => IncompleteCommitLogReadPolicy.withName(prop))

    val commitLogCloseDelayMs =
      helper.castCheck("commit.log.close.delay-ms", prop => prop.toInt)

    val commitLogFileTtlSec =
      helper.castCheck("commit.log.file.ttl-sec", prop => prop.toInt)


    CommitLogOptions(
      commitLogWriteSyncPolicy,
      commitLogWriteSyncValue,
      incompleteCommitLogReadPolicy,
      commitLogCloseDelayMs,
      commitLogFileTtlSec
    )
  }

  private def loadZookeeperSpecificOptions() = {
    implicit val typeTag = classOf[ServerOptions.ZooKeeperOptions]

    val counterPathFileIdGen =
      helper.castCheck("counter.path.file.id.gen", identity)

    ServerOptions.ZooKeeperOptions(
      counterPathFileIdGen
    )
  }

  def getServerAuthOptions = {
    serverAuthOptions
  }

  def getZookeeperOptions = {
    zookeeperOptions
  }

  def getBootstrapOptions = {
    bootstrapOptions
  }

  def getServerReplicationOptions = {
    serverReplicationOptions
  }

  def getServerStorageOptions = {
    serverStorageOptions
  }

  def getServerRocksStorageOptions = {
    serverRocksStorageOptions
  }

  def getPackageTransmissionOptions = {
    packageTransmissionOptions
  }

  def getCommitLogOptions = {
    commitLogOptions
  }

  def getZookeeperSpecificOptions = {
    zookeeperSpecificOptions
  }

  def getSubscribersUpdateOptions = {
    subscribersUpdateOptions
  }
}