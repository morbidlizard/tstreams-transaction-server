package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.options.ClientOptions._
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions


class ClientBuilder private(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, connectionOpts: ConnectionOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val connectionOptions = connectionOpts

  def this() = this(AuthOptions(), ZookeeperOptions(), ConnectionOptions())

  def withAuthOptions(authOptions: AuthOptions) = new ClientBuilder(authOptions, zookeeperOptions, connectionOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) = new ClientBuilder(authOptions, zookeeperOptions, connectionOptions)

  def withConnectionOptions(clientOptions: ConnectionOptions) = new ClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def build() = new Client(connectionOptions, authOptions, zookeeperOptions)

  def getConnectionOptions = connectionOptions.copy()

  def getZookeeperOptions = zookeeperOptions.copy()

  def getAuthOptions = authOptions.copy()
}