package com.bwsw.tstreamstransactionserver.netty.server

import java.io.File

import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.sleepycat.je.{Durability, Environment, EnvironmentConfig}

trait HasEnvironment {
  val storageOpts: StorageOptions
  val environment: Environment = {
    val directory = new File(storageOpts.path, storageOpts.metadataDirectory)
    directory.mkdirs()
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