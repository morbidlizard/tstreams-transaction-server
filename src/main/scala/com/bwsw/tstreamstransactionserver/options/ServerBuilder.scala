package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._

class ServerBuilder private(authOpts: AuthenticationOptions, zookeeperOpts: CommonOptions.ZookeeperOptions,
                            bootstrapOpts: BootstrapOptions, serverReplicationOpts: ServerReplicationOptions,
                            storageOpts: StorageOptions, rocksStorageOpts: RocksStorageOptions, commitLogOpts: CommitLogOptions,
                            packageTransmissionOpts: TransportOptions, zookeeperSpecificOpt: ServerOptions.ZooKeeperOptions,
                            subscriberUpdateOpts: SubscriberUpdateOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val serverReplicationOptions = serverReplicationOpts
  private val storageOptions = storageOpts
  private val rocksStorageOptions = rocksStorageOpts
  private val commitLogOptions = commitLogOpts
  private val packageTransmissionOptions = packageTransmissionOpts
  private val zookeeperSpecificOptions = zookeeperSpecificOpt
  private val subscribersUpdateOptions = subscriberUpdateOpts

  def this() = this(
    AuthenticationOptions(), CommonOptions.ZookeeperOptions(),
    BootstrapOptions(), ServerReplicationOptions(),
    StorageOptions(), RocksStorageOptions(), CommitLogOptions(),
    TransportOptions(), ServerOptions.ZooKeeperOptions(),
    SubscriberUpdateOptions()
  )

  def withAuthOptions(authOptions: AuthenticationOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscribersUpdateOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscribersUpdateOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscribersUpdateOptions)

  def withServerReplicationOptions(serverReplicationOptions: ServerReplicationOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscribersUpdateOptions)

  def withServerStorageOptions(serverStorageOptions: StorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, serverStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscribersUpdateOptions)

  def withServerRocksStorageOptions(serverStorageRocksOptions: RocksStorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, serverStorageRocksOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscribersUpdateOptions)

  def withCommitLogOptions(commitLogOptions: CommitLogOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscribersUpdateOptions)

  def withPackageTransmissionOptions(packageTransmissionOptions: TransportOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscribersUpdateOptions)

  def withZooKeeperSpecificOption(zookeeperSpecificOptions: ServerOptions.ZooKeeperOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscribersUpdateOptions)

  def withSubscribersUpdateOptions(subscriberUpdateOptions: SubscriberUpdateOptions): ServerBuilder =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, zookeeperSpecificOptions, subscriberUpdateOptions)


  def build() = new Server(
    authOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions,
    storageOptions, rocksStorageOptions, commitLogOptions,
    packageTransmissionOptions, zookeeperSpecificOptions,
    subscribersUpdateOptions
  )

  def getZookeeperOptions = zookeeperOptions.copy()

  def getAuthOptions = authOptions.copy()

  def getBootstrapOptions = bootstrapOptions.copy()

  def getServerReplicationOptions = serverReplicationOptions.copy()

  def getStorageOptions = storageOptions.copy()

  def getRocksStorageOptions = rocksStorageOptions.copy()

  def getPackageTransmissionOptions = packageTransmissionOptions.copy()

  def getCommitLogOptions = commitLogOptions.copy()

  def getZookeeperSpecificOptions = zookeeperSpecificOptions.copy()

  def getSubscribersUpdateOptions = subscribersUpdateOptions.copy()
}