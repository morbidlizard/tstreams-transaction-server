package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg

import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions._
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._

class CheckpointGroupServerBuilder private(authenticationOpts: AuthenticationOptions,
                                           packageTransmissionOpts: TransportOptions,
                                           zookeeperOpts: CommonOptions.ZookeeperOptions,
                                           bootstrapOpts: BootstrapOptions,
                                           checkpointGroupRoleOpts: CheckpointGroupRoleOptions,
                                           checkpointGroupPrefixesOpts: CheckpointGroupPrefixesOptions,
                                           bookkeeperOpts: BookkeeperOptions,
                                           storageOpts: StorageOptions,
                                           rocksStorageOpts: RocksStorageOptions) {

  private val authenticationOptions = authenticationOpts
  private val packageTransmissionOptions = packageTransmissionOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val checkpointGroupRoleOptions = checkpointGroupRoleOpts
  private val checkpointGroupPrefixesOptions = checkpointGroupPrefixesOpts
  private val bookkeeperOptions = bookkeeperOpts
  private val storageOptions = storageOpts
  private val rocksStorageOptions = rocksStorageOpts

  def this() = this(
    AuthenticationOptions(),
    TransportOptions(),
    CommonOptions.ZookeeperOptions(),
    BootstrapOptions(),
    CheckpointGroupRoleOptions(),
    CheckpointGroupPrefixesOptions(),
    BookkeeperOptions(),
    StorageOptions(),
    RocksStorageOptions()
  )

  def withAuthenticationOptions(authenticationOptions: AuthenticationOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions)

  def withPackageTransmissionOptions(packageTransmissionOptions: TransportOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions)

  def withCheckpointGroupRoleOptions(checkpointGroupRoleOptions: CheckpointGroupRoleOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions)

  def withCheckpointGroupPrefixesOptions(checkpointGroupPrefixesOptions: CheckpointGroupPrefixesOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions)

  def withBookkeeperOptions(bookkeeperOptions: BookkeeperOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions)

  def withServerStorageOptions(storageOptions: StorageOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions)

  def withServerRocksStorageOptions(rocksStorageOptions: RocksStorageOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions)


  def build() = new CheckpointGroupServer(
    authenticationOptions,
    packageTransmissionOptions,
    zookeeperOptions,
    bootstrapOptions,
    checkpointGroupRoleOptions,
    checkpointGroupPrefixesOptions,
    bookkeeperOptions,
    storageOptions,
    rocksStorageOptions
  )

  def getAuthenticationOptions =
    authenticationOptions.copy()

  def getPackageTransmissionOptions =
    packageTransmissionOptions.copy()

  def getZookeeperOptions =
    zookeeperOptions.copy()

  def getBootstrapOptions =
    bootstrapOptions.copy()

  def getCheckpointGroupRoleOptions =
    checkpointGroupRoleOptions.copy()

  def getCheckpointGroupPrefixesOptions =
    checkpointGroupPrefixesOptions.copy()

  def getBookkeeperOptions =
    bookkeeperOptions.copy()

  def getStorageOptions =
    storageOptions.copy()

  def getRocksStorageOptions =
    rocksStorageOptions.copy()
}
