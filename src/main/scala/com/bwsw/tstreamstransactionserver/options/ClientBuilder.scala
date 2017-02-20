package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.client.Client
import ClientOptions._
import CommonOptions._

class ClientBuilder private(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, clientOpts: ConnectionOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val clientOptions = clientOpts

  def this() = this(AuthOptions(), ZookeeperOptions(), ConnectionOptions())

  def withAuthOptions(authOptions: AuthOptions) = new ClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) = new ClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def withClientOptions(clientOptions: ConnectionOptions) = new ClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def build() = new Client(clientOptions, authOptions, zookeeperOptions)

  def getClientOptions() = clientOptions.copy()

  def getZookeeperOptions() = zookeeperOptions.copy()

  def getAuthOptions() = authOptions.copy()
}