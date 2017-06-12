package com.bwsw.tstreamstransactionserver

import com.bwsw.tstreamstransactionserver.options._

object SingleNodeServerLauncher {

  def main(args: Array[String]): Unit = {
    val optionsLoader = new OptionsLoader()

    val builder = new SingleNodeServerBuilder()
    val server = builder
      .withBootstrapOptions(optionsLoader.getBootstrapOptions)
      .withSubscribersUpdateOptions(optionsLoader.getSubscribersUpdateOptions)
      .withAuthenticationOptions(optionsLoader.getServerAuthenticationOptions)
      .withServerReplicationOptions(optionsLoader.getServerReplicationOptions)
      .withServerStorageOptions(optionsLoader.getServerStorageOptions)
      .withServerRocksStorageOptions(optionsLoader.getServerRocksStorageOptions)
      .withZookeeperOptions(optionsLoader.getZookeeperOptions)
      .withPackageTransmissionOptions(optionsLoader.getPackageTransmissionOptions)
      .withCommitLogOptions(optionsLoader.getCommitLogOptions)
      .build()

    server.start()
  }
}