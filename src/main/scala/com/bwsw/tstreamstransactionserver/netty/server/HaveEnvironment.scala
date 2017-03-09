package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import com.sleepycat.je.{Durability, Environment, EnvironmentConfig}

trait HaveEnvironment {
  val storageOpts: StorageOptions
  val environment: Environment = {
    val directory = FileUtils.createDirectory(storageOpts.metadataDirectory, storageOpts.path)
    val defaultDurability = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE)
    val environment = {
      val environmentConfig = new EnvironmentConfig()
        .setAllowCreate(true)
        .setTransactional(true)
        .setSharedCache(true)

      //    config.berkeleyDBJEproperties foreach {
      //      case (name, value) => environmentConfig.setConfigParam(name,value)
      //    } //todo it will be deprecated soon


      environmentConfig.setDurabilityVoid(defaultDurability)

      new Environment(directory, environmentConfig)
    }
    environment
  }
}
