package com.bwsw.tstreamstransactionserver

import com.bwsw.tstreamstransactionserver.options._

object ServerLauncher {

  def main(args: Array[String]): Unit = {
    val optionsLoader = new OptionsLoader()

    val builder = new ServerBuilder()
    val server = builder
      .withBootstrapOptions(optionsLoader.getBootstrapOptions())
      .withAuthOptions(optionsLoader.getServerAuthOptions())
      .withServerReplicationOptions(optionsLoader.getServerReplicationOptions())
      .withServerStorageOptions(optionsLoader.getServerStorageOptions())
      .withBerkeleyStorageOptions(optionsLoader.getBerkeleyStoragaeOptions())
      .withServerRocksStorageOptions(optionsLoader.getServerRocksStorageOptions())
      .withZookeeperOptions(optionsLoader.getZookeeperOptions())
      .withPackageTransmissionOptions(optionsLoader.getPackageTransmissionOptions())
      .withCommitLogOptions(optionsLoader.getCommitLogOptions())
      .build()

    server.start()
  }
}