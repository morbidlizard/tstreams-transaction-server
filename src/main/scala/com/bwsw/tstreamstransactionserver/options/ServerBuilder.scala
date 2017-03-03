package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.server.Server
import ServerOptions._
import CommonOptions.ZookeeperOptions

class ServerBuilder private(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, bootstrapOpts: BootstrapOptions,
                            storageOpts: StorageOptions, serverReplicationOpts: ServerReplicationOptions,
                            rocksStorageOpts: RocksStorageOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val storageOptions = storageOpts
  private val serverReplicationOptions = serverReplicationOpts
  private val rocksStorageOptions = rocksStorageOpts

  def this() = this(AuthOptions(), ZookeeperOptions(), BootstrapOptions(), StorageOptions(), ServerReplicationOptions(), RocksStorageOptions())

  def withAuthOptions(authOptions: AuthOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions)

  def withBootstrapOptions(serverOptions: BootstrapOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, serverOptions, storageOptions, serverReplicationOptions, rocksStorageOptions)

  def withServerStorageOptions(serverStorageOptions: StorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverStorageOptions, serverReplicationOptions, rocksStorageOptions)

  def withServerReplicationOptions(serverReplicationOptions: ServerReplicationOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions)

  def withServerStorageRocksOptions(serverStorageRocksOptions: RocksStorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, serverStorageRocksOptions)

  def build() = new Server(authOptions, zookeeperOptions, bootstrapOptions,
    storageOptions, serverReplicationOptions, rocksStorageOptions)

  def getZookeeperOptions() = zookeeperOptions.copy()

  def getAuthOptions() = authOptions.copy()

  def getBootstrapOptions() = bootstrapOptions.copy()

  def getStorageOptions() = storageOptions.copy()

  def getServerReplicationOptions() = serverReplicationOptions.copy()

  def getRocksStorageOptions() = rocksStorageOptions.copy()
}