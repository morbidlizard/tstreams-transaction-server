package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.server.SingleNodeServer
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._

class SingleNodeServerBuilder private(authenticationOpts: AuthenticationOptions,
                                      zookeeperOpts: CommonOptions.ZookeeperOptions,
                                      bootstrapOpts: BootstrapOptions,
                                      serverReplicationOpts: ServerReplicationOptions,
                                      storageOpts: StorageOptions,
                                      rocksStorageOpts: RocksStorageOptions,
                                      commitLogOpts: CommitLogOptions,
                                      packageTransmissionOpts: TransportOptions,
                                      subscriberUpdateOpts: SubscriberUpdateOptions) {

  private val authenticationOptions = authenticationOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val serverReplicationOptions = serverReplicationOpts
  private val storageOptions = storageOpts
  private val rocksStorageOptions = rocksStorageOpts
  private val commitLogOptions = commitLogOpts
  private val packageTransmissionOptions = packageTransmissionOpts
  private val subscribersUpdateOptions = subscriberUpdateOpts

  def this() = this(
    AuthenticationOptions(), CommonOptions.ZookeeperOptions(),
    BootstrapOptions(), ServerReplicationOptions(),
    StorageOptions(), RocksStorageOptions(), CommitLogOptions(),
    TransportOptions(),
    SubscriberUpdateOptions()
  )

  def withAuthenticationOptions(authenticationOptions: AuthenticationOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions)

  def withServerReplicationOptions(serverReplicationOptions: ServerReplicationOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions)

  def withServerStorageOptions(serverStorageOptions: StorageOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, serverStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions)

  def withServerRocksStorageOptions(serverStorageRocksOptions: RocksStorageOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, serverStorageRocksOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions)

  def withCommitLogOptions(commitLogOptions: CommitLogOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions)

  def withPackageTransmissionOptions(packageTransmissionOptions: TransportOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions)

  def withSubscribersUpdateOptions(subscriberUpdateOptions: SubscriberUpdateOptions): SingleNodeServerBuilder =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscriberUpdateOptions)


  def build() = new SingleNodeServer(
    authenticationOptions, zookeeperOptions, bootstrapOptions, serverReplicationOptions,
    storageOptions, rocksStorageOptions, commitLogOptions,
    packageTransmissionOptions,
    subscribersUpdateOptions
  )

  def getZookeeperOptions = zookeeperOptions.copy()

  def getAuthenticationOptions = authenticationOptions.copy()

  def getBootstrapOptions = bootstrapOptions.copy()

  def getServerReplicationOptions = serverReplicationOptions.copy()

  def getStorageOptions = storageOptions.copy()

  def getRocksStorageOptions = rocksStorageOptions.copy()

  def getPackageTransmissionOptions = packageTransmissionOptions.copy()

  def getCommitLogOptions = commitLogOptions.copy()

  def getSubscribersUpdateOptions = subscribersUpdateOptions.copy()
}