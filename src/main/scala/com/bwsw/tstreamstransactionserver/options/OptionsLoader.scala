
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bwsw.tstreamstransactionserver.options

import java.io.FileInputStream
import java.util.Properties

import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import org.rocksdb.CompressionType


class OptionsLoader {
  require(System.getProperty(CommonOptions.PROPERTY_FILE_NAME) != null,
    s"There is no file with properties. " +
      s"You should define a path to a property file through '-D${CommonOptions.PROPERTY_FILE_NAME}=<path_to_file>' " +
      s"(e.g. 'java -D${CommonOptions.PROPERTY_FILE_NAME}=/home/user/config.properties " +
      s"-cp target/scala-2.12/tstreams-transaction-server-1.3.8.1-SNAPSHOT.jar:/home/user/slf4j-api-1.7.24.jar:/home/user/slf4j-simple-1.7.24.jar " +
      "com.bwsw.tstreamstransactionserver.ServerLauncher').")

  private val props = new Properties()
  props.load(new FileInputStream(System.getProperty(CommonOptions.PROPERTY_FILE_NAME)))

  private val serverAuthOptions = loadServerAuthenticationOptions()
  private val zookeeperOptions = loadZookeeperOptions()
  private val bootstrapOptions = loadBootstrapOptions()
  private val serverReplicationOptions = loadServerReplicationOptions()
  private val serverStorageOptions = loadServerStorageOptions()
  private val serverRocksStorageOptions = loadServerRocksStorageOptions()
  private val packageTransmissionOptions = loadPackageTransmissionOptions()
  private val commitLogOptions = loadCommitLogOptions()
  private val subscribersUpdateOptions = loadSubscribersUpdateOptions()

  private lazy val helper = new OptionHelper(props)

  private def loadBootstrapOptions() = {
    implicit val typeTag = classOf[BootstrapOptions]

    val bindHost =
      helper.castCheck("bootstrap.host", identity)
    val bindPort =
      helper.castCheck("bootstrap.port", prop => prop.toInt)
    val openOperationsPoolSize =
      helper.castCheck("bootstrap.open-ops-pool-size", prop => prop.toInt)

    BootstrapOptions(bindHost, bindPort, openOperationsPoolSize)
  }

  private def loadSubscribersUpdateOptions() = {
    implicit val typeTag = classOf[SubscriberUpdateOptions]

    val updatePeriodMs =
      helper.castCheck("subscribers.update-period-ms", prop => prop.toInt)
    val subscriberMonitoringZkEndpoints =
      scala.util.Try(
        helper.castCheck("subscribers.monitoring-zk-endpoints", identity)
      ).toOption

    SubscriberUpdateOptions(updatePeriodMs, subscriberMonitoringZkEndpoints)
  }

  private def loadServerAuthenticationOptions() = {
    implicit val typeTag = classOf[ServerOptions.AuthenticationOptions]

    val key =
      helper.castCheck("authentication.key", identity)
    val keyCacheSize =
      helper.castCheck("authentication.key-cache-size", prop => prop.toInt)
    val keyCacheExpirationTimeSec =
      helper.castCheck("authentication.key-cache-expiration-time-sec", prop => prop.toInt)

    ServerOptions.AuthenticationOptions(key, keyCacheSize, keyCacheExpirationTimeSec)
  }

  private def loadServerStorageOptions() = {
    implicit val typeTag = classOf[StorageOptions]

    val path =
      helper.castCheck("storage-model.file-prefix", identity)

    val streamZookeeperDirectory =
      helper.castCheck("storage-model.streams.zk-directory", identity)

    val dataDirectory =
      helper.castCheck("storage-model.data.directory", identity)

    val metadataDirectory =
      helper.castCheck("storage-model.metadata.directory", identity)

    val commitLogRawDirectory =
      helper.castCheck("storage-model.commit-log.raw-directory", identity)

    val commitLogRocksDirectory =
      helper.castCheck("storage-model.commit-log.rocks-directory", identity)

    StorageOptions(
      path,
      streamZookeeperDirectory,
      dataDirectory,
      metadataDirectory,
      commitLogRawDirectory,
      commitLogRocksDirectory
    )
  }

  private def loadServerReplicationOptions() = {
    ServerReplicationOptions()
  }

  private def loadServerRocksStorageOptions() = {
    implicit val typeTag = classOf[RocksStorageOptions]

    val writeThreadPool =
      helper.castCheck("rocksdb.write-thread-pool", prop => prop.toInt)

    val readThreadPool =
      helper.castCheck("rocksdb.read-thread-pool",  prop => prop.toInt)

    val transactionTtlAppendMs =
      helper.castCheck("rocksdb.transaction-ttl-append-ms",  prop => prop.toInt)

    val transactionExpungeDelayMin =
      helper.castCheck("rocksdb.transaction-expunge-delay-min",  prop => prop.toInt)

    val maxBackgroundCompactions =
      helper.castCheck("rocksdb.max-background-compactions",  prop => prop.toInt)

    val compressionType =
      helper.castCheck("rocksdb.compression-type", prop => CompressionType.getCompressionType(prop))

    val isFsync =
      helper.castCheck("rocksdb.is-fsync", prop => prop.toBoolean)

    RocksStorageOptions(
      writeThreadPool,
      readThreadPool,
      transactionTtlAppendMs,
      transactionExpungeDelayMin,
      maxBackgroundCompactions,
      compressionType,
      isFsync
    )
  }

  private def loadZookeeperOptions() = {
    implicit val typeTag = classOf[ZookeeperOptions]

    val endpoints =
      helper.castCheck("zk.endpoints", identity)

    val prefix =
      helper.castCheck("zk.prefix", identity)

    val sessionTimeoutMs =
      helper.castCheck("zk.session-timeout-ms", prop => prop.toInt)

    val retryDelayMs =
      helper.castCheck("zk.connection-retry-delay-ms", prop => prop.toInt)

    val connectionTimeoutMs =
      helper.castCheck("zk.connection-timeout-ms", prop => prop.toInt)

    ZookeeperOptions(endpoints, prefix, sessionTimeoutMs, retryDelayMs, connectionTimeoutMs)
  }

  private def loadPackageTransmissionOptions() = {
    implicit val typeTag = classOf[TransportOptions]

    val maxMetadataPackageSize =
      helper.castCheck("network.max-metadata-package-size", prop => prop.toInt)

    val maxDataPackageSize =
      helper.castCheck("network.max-data-package-size", prop => prop.toInt)

    TransportOptions(maxMetadataPackageSize, maxDataPackageSize)
  }

  private def loadCommitLogOptions() = {
    implicit val typeTag = classOf[CommitLogOptions]

    val writeSyncPolicy =
      helper.castCheck("commit-log.write-sync-policy", prop => CommitLogWriteSyncPolicy.withName(prop))

    val writeSyncValue =
      helper.castCheck("commit-log.write-sync-value", prop => prop.toInt)

    val incompleteReadPolicy =
      helper.castCheck("commit-log.incomplete-read-policy", prop => IncompleteCommitLogReadPolicy.withName(prop))

    val closeDelayMs =
      helper.castCheck("commit-log.close-delay-ms", prop => prop.toInt)

    val expungeDelaySec =
      helper.castCheck("commit-log.rocksdb-expunge-delay-sec", prop => prop.toInt)

    val zkFileIdGeneratorPath =
      helper.castCheck("commit-log.zk-file-id-gen-path", identity)

    CommitLogOptions(
      writeSyncPolicy,
      writeSyncValue,
      incompleteReadPolicy,
      closeDelayMs,
      expungeDelaySec,
      zkFileIdGeneratorPath
    )
  }

  def getServerAuthenticationOptions = {
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

  def getSubscribersUpdateOptions = {
    subscribersUpdateOptions
  }
}