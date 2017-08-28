package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CommonPrefixesOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._

class CommonCheckpointGroupServerBuilder private(authenticationOpts: AuthenticationOptions,
                                                 packageTransmissionOpts: TransportOptions,
                                                 zookeeperOpts: CommonOptions.ZookeeperOptions,
                                                 bootstrapOpts: BootstrapOptions,
                                                 commonRoleOpts: CommonRoleOptions,
                                                 commonPrefixesOpts: CommonPrefixesOptions,
                                                 checkpointGroupRoleOpts: CheckpointGroupRoleOptions,
                                                 bookkeeperOpts: BookkeeperOptions,
                                                 storageOpts: StorageOptions,
                                                 rocksStorageOpts: RocksStorageOptions,
                                                 subscriberUpdateOpts: SubscriberUpdateOptions) {

  private val authenticationOptions = authenticationOpts
  private val packageTransmissionOptions = packageTransmissionOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val commonRoleOptions = commonRoleOpts
  private val commonPrefixesOptions = commonPrefixesOpts
  private val checkpointGroupRoleOptions = checkpointGroupRoleOpts
  private val bookkeeperOptions = bookkeeperOpts
  private val storageOptions = storageOpts
  private val rocksStorageOptions = rocksStorageOpts
  private val subscribersUpdateOptions = subscriberUpdateOpts

  def this() = this(
    AuthenticationOptions(),
    TransportOptions(),
    CommonOptions.ZookeeperOptions(),
    BootstrapOptions(),
    CommonRoleOptions(),
    CommonPrefixesOptions(),
    CheckpointGroupRoleOptions(),
    BookkeeperOptions(),
    StorageOptions(),
    RocksStorageOptions(),
    SubscriberUpdateOptions()
  )

  def withAuthenticationOptions(authenticationOptions: AuthenticationOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withPackageTransmissionOptions(packageTransmissionOptions: TransportOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withCommonRoleOptions(commonRoleOptions: CommonRoleOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withCommonPrefixesOptions(commonPrefixesOptions: CommonPrefixesOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withCheckpointGroupRoleOptions(checkpointGroupRoleOptions: CheckpointGroupRoleOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withBookkeeperOptions(bookkeeperOptions: BookkeeperOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withServerStorageOptions(storageOptions: StorageOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withServerRocksStorageOptions(rocksStorageOptions: RocksStorageOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def withSubscribersUpdateOptions(subscribersUpdateOptions: SubscriberUpdateOptions) =
    new CommonCheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, commonPrefixesOptions, checkpointGroupRoleOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, subscribersUpdateOptions)

  def build() = new CommonCheckpointGroupServer(
    authenticationOptions,
    packageTransmissionOptions,
    zookeeperOptions,
    bootstrapOptions,
    commonRoleOptions,
    commonPrefixesOptions,
    checkpointGroupRoleOptions,
    bookkeeperOptions,
    storageOptions,
    rocksStorageOptions,
    subscribersUpdateOptions
  )

  def getAuthenticationOptions =
    authenticationOptions.copy()

  def getPackageTransmissionOptions =
    packageTransmissionOptions.copy()

  def getZookeeperOptions =
    zookeeperOptions.copy()

  def getBootstrapOptions =
    bootstrapOptions.copy()

  def getCommonRoleOptions =
    commonRoleOptions.copy()

  def getCommonPrefixesOptions =
    commonPrefixesOptions.copy()

  def getCheckpointGroupRoleOptions =
    checkpointGroupRoleOptions.copy()

  def getBookkeeperOptions =
    bookkeeperOptions.copy()

  def getStorageOptions =
    storageOptions.copy()

  def getRocksStorageOptions =
    rocksStorageOptions.copy()

  def getSubscribersUpdateOptions =
    subscribersUpdateOptions.copy()
}


