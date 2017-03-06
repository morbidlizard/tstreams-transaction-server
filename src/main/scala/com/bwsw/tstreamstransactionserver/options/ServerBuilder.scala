package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.server.Server
import ServerOptions._
import CommonOptions.ZookeeperOptions

class ServerBuilder private(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, bootstrapOpts: BootstrapOptions,
                            storageOpts: StorageOptions, serverReplicationOpts: ServerReplicationOptions,
                            rocksStorageOpts: RocksStorageOptions, commitLogOpts: CommitLogOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val storageOptions = storageOpts
  private val serverReplicationOptions = serverReplicationOpts
  private val rocksStorageOptions = rocksStorageOpts
  private val commitLogOptions = commitLogOpts

  def this() = this(AuthOptions(), ZookeeperOptions(), BootstrapOptions(), StorageOptions(), ServerReplicationOptions(), RocksStorageOptions(), CommitLogOptions())

  def withAuthOptions(authOptions: AuthOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions)

  def withServerStorageOptions(serverStorageOptions: StorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverStorageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions)

  def withServerReplicationOptions(serverReplicationOptions: ServerReplicationOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions)

  def withServerRocksStorageOptions(serverRocksStorageOptions: RocksStorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, serverRocksStorageOptions, commitLogOptions)

   def withCommitLogOptions(commitLogOptions: CommitLogOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions)

  def build() = new Server(authOptions, zookeeperOptions, bootstrapOptions,
    storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions)

  def getZookeeperOptions() = zookeeperOptions.copy()

  def getAuthOptions() = authOptions.copy()

  def getBootstrapOptions() = bootstrapOptions.copy()

  def getStorageOptions() = storageOptions.copy()

  def getServerReplicationOptions() = serverReplicationOptions.copy()

  def getRocksStorageOptions() = rocksStorageOptions.copy()

  def getCommitLogOptions() = commitLogOptions.copy()
}