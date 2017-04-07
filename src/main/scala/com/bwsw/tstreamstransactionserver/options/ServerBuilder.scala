package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._

class ServerBuilder private(authOpts: AuthOptions, zookeeperOpts: CommonOptions.ZookeeperOptions,
                            bootstrapOpts: BootstrapOptions, serverReplicationOpts: ServerReplicationOptions,
                            storageOpts: StorageOptions, berkeleyStorageOpts: BerkeleyStorageOptions, rocksStorageOpts: RocksStorageOptions, commitLogOpts: CommitLogOptions,
                            packageTransmissionOpts: TransportOptions, zookeeperSpecificOpt: ServerOptions.ZooKeeperOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val serverReplicationOptions = serverReplicationOpts
  private val storageOptions = storageOpts
  private val berkeleyStorageOptions = berkeleyStorageOpts
  private val rocksStorageOptions = rocksStorageOpts
  private val commitLogOptions = commitLogOpts
  private val packageTransmissionOptions = packageTransmissionOpts
  private val zookeeperSpecificOptions = zookeeperSpecificOpt

  def this() = this(
    AuthOptions(), CommonOptions.ZookeeperOptions(),
    BootstrapOptions(), ServerReplicationOptions(),
    StorageOptions(), BerkeleyStorageOptions(), RocksStorageOptions(), CommitLogOptions(),
    TransportOptions(), ServerOptions.ZooKeeperOptions()
  )

  def withAuthOptions(authOptions: AuthOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)

  def withServerReplicationOptions(serverReplicationOptions: ServerReplicationOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)

  def withServerStorageOptions(serverStorageOptions: StorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, serverStorageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)

  def withBerkeleyStorageOptions(berkeleyStorageOptions: BerkeleyStorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)

  def withServerRocksStorageOptions(serverStorageRocksOptions: RocksStorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, serverStorageRocksOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)

  def withCommitLogOptions(commitLogOptions: CommitLogOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)

  def withPackageTransmissionOptions(packageTransmissionOptions: TransportOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)

  def withZooKeeperSpecificOption(zookeeperSpecificOptions: ServerOptions.ZooKeeperOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions)


  def build() = new Server(
    authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions,
    storageOptions, berkeleyStorageOptions, rocksStorageOptions, commitLogOptions,
    packageTransmissionOptions, zookeeperSpecificOptions
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

  def getZookeeperSpecificOptions = zookeeperSpecificOptions.copy()
}