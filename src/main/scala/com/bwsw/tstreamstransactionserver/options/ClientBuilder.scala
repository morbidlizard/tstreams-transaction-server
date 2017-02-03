package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.Server
import org.rocksdb.{CompressionType, Options}

/**
  *
  * @param endpoints
  * @param prefix
  * @param sessionTimeoutMs
  * @param retryCount
  * @param retryDelayMs
  * @param connectionTimeoutMs
  */
case class ZookeeperOptions(endpoints: String = "127.0.0.1:2181", prefix: String = "/tts", sessionTimeoutMs: Int = 10000,
                            retryCount: Int = 5, retryDelayMs: Int = 500, connectionTimeoutMs: Int = 10000)

/**
  *
  * @param key
  * @param connectionTimeoutMs
  * @param retryDelayMs
  * @param tokenRetryDelayMs
  * @param tokenConnectionTimeoutMs
  */
case class AuthOptions(key: String = "", connectionTimeoutMs: Int = 5000, retryDelayMs: Int = 500,
                       tokenRetryDelayMs: Int = 200, tokenConnectionTimeoutMs: Int = 5000, ttl: Int = 120, cacheSize: Int = 100)

/**
  *
  * @param connectionTimeoutMs
  * @param retryDelayMs
  * @param threadPool
  */
case class ClientOptions(connectionTimeoutMs: Int = 5000, retryDelayMs: Int = 200, threadPool: Int = 4)

case class ServerOptions(host: String = "127.0.0.1", port: Int = 8071, threadPool: Int = 4)

case class StorageOptions(path: String = "/tmp", clearDelayMs: Int = 10, clearAmount: Int = 200,
                          streamDirectory: String = "stream", consumerDirectory: String = "consumer",
                          dataDirectory: String = "transaction_data", metadataDirectory: String = "transaction_metadata",
                          streamStorageName: String = "StreamStore", consumerStorageName: String = "ConsumerStore",
                          metadataStorageName: String = "TransactionStore",
                          openedTransactionsStorageName: String = "TransactionOpenStore",
                          berkeleyReadThreadPool: Int = 2, ttlAddMs: Int = 50)

case class ServerReplicationOptions(endpoints: String = "127.0.0.1:8071", name: String = "server", group: String = "group")

case class RocksStorageOptions(writeThreadPool: Int = 4, readThreadPoll: Int = 2, ttlAddMs: Int = 50,
                               createIfMissing: Boolean = true, maxBackgroundCompactions: Int = 1,
                               allowOsBuffer: Boolean = true, compression: CompressionType = CompressionType.LZ4_COMPRESSION,
                               useFsync: Boolean = true) {
  private lazy val options = new Options().setCreateIfMissing(createIfMissing)
    .setMaxBackgroundCompactions(maxBackgroundCompactions)
    .setAllowOsBuffer(allowOsBuffer)
    .setCompressionType(compression)
    .setUseFsync(useFsync)

  def getDBOptions(): Options = {
    options
  }
}

class ClientBuilder private(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, clientOpts: ClientOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val clientOptions = clientOpts

  def this() = this(AuthOptions(), ZookeeperOptions(), ClientOptions())

  def withAuthOptions(authOptions: AuthOptions) = new ClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) = new ClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def withClientOptions(clientOptions: ClientOptions) = new ClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def build() = new Client(clientOptions, authOptions, zookeeperOptions)

  def getClientOptions() = clientOptions.copy()

  def getZookeeperOptions() = zookeeperOptions.copy()

  def getAuthOptions() = authOptions.copy()
}

class ServerBuilder private(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, serverOpts: ServerOptions,
                            storageOpts: StorageOptions, serverReplicationOpts: ServerReplicationOptions,
                            rocksStorageOpts: RocksStorageOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val serverOptions = serverOpts
  private val storageOptions = storageOpts
  private val serverReplicationOptions = serverReplicationOpts
  private val rocksStorageOptions = rocksStorageOpts

  def this() = this(AuthOptions(), ZookeeperOptions(), ServerOptions(), StorageOptions(), ServerReplicationOptions(), RocksStorageOptions())

  def withAuthOptions(authOptions: AuthOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, serverOptions, storageOptions, serverReplicationOptions, rocksStorageOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, serverOptions, storageOptions, serverReplicationOptions, rocksStorageOptions)

  def withServerOptions(serverOptions: ServerOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, serverOptions, storageOptions, serverReplicationOptions, rocksStorageOptions)

  def withServerStorageOptions(serverStorageOptions: StorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, serverOptions, serverStorageOptions, serverReplicationOptions, rocksStorageOptions)

  def withServerReplicationOptions(serverReplicationOptions: ServerReplicationOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, serverOptions, storageOptions, serverReplicationOptions, rocksStorageOptions)

  def withServerStorageRocksOptions(serverStorageRocksOptions: RocksStorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, serverOptions, storageOptions, serverReplicationOptions, serverStorageRocksOptions)

  def build() = new Server(authOptions, zookeeperOptions, serverOptions,
    storageOptions, serverReplicationOptions, rocksStorageOptions)

  def getZookeeperOptions() = zookeeperOptions.copy()

  def getAuthOptions() = authOptions.copy()

  def getServerOptions() = serverOptions.copy()

  def getStorageOptions() = storageOptions.copy()

  def getServerReplicationOptions() = serverReplicationOptions.copy()

  def getRocksStorageOptions() = rocksStorageOptions.copy()
}