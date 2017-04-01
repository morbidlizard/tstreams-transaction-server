package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._

class ServerBuilder private(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions,
                            bootstrapOpts: BootstrapOptions, serverReplicationOpts: ServerReplicationOptions,
                            storageOpts: StorageOptions, berkeleyStorageOpts: BerkeleyStorageOptions, rocksStorageOpts: RocksStorageOptions, commitLogOpts: CommitLogOptions,
                            packageTransmissionOpts: TransportOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val serverReplicationOptions = serverReplicationOpts
  private val storageOptions = storageOpts
  private val berkeleyStorageOptions = berkeleyStorageOpts
  private val rocksStorageOptions = rocksStorageOpts
  private val commitLogOptions = commitLogOpts
  private val packageTransmissionOptions = packageTransmissionOpts

  def this() = this(
    AuthOptions(), ZookeeperOptions(),
    BootstrapOptions(), ServerReplicationOptions(),
    StorageOptions(), BerkeleyStorageOptions(), RocksStorageOptions(), CommitLogOptions(),
    TransportOptions()
  )

  def withAuthOptions(authOptions: AuthOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withServerReplicationOptions(serverReplicationOptions: ServerReplicationOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withServerStorageOptions(serverStorageOptions: StorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, serverStorageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withBerkeleyStorageOptions(berkeleyStorageOptions: BerkeleyStorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withServerRocksStorageOptions(serverStorageRocksOptions: RocksStorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, serverStorageRocksOptions, commitLogOptions, packageTransmissionOptions)

  def withCommitLogOptions(commitLogOptions: CommitLogOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withPackageTransmissionOptions(packageTransmissionOptions: TransportOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)


  def build() = new Server(
    authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions,
    storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions,
    packageTransmissionOptions
  )

  def getZookeeperOptions = zookeeperOptions.copy()

  def getAuthOptions = authOptions.copy()

  def getBootstrapOptions = bootstrapOptions.copy()

  def getServerReplicationOptions = serverReplicationOptions.copy()

  def getStorageOptions = storageOptions.copy()

  def getBerkeleyStorageOptions = berkeleyStorageOptions.copy()

  def getRocksStorageOptions = rocksStorageOptions.copy()

  def getPackageTransmissionOptions = packageTransmissionOptions.copy()

  def getCommitLogOptions = commitLogOptions.copy()
}