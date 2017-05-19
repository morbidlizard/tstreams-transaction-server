package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.options.ClientOptions._
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import org.apache.curator.framework.CuratorFramework


class ClientBuilder private(authOpts: AuthOptions,
                            zookeeperOpts: ZookeeperOptions,
                            connectionOpts: ConnectionOptions,
                            curatorOpt: Option[CuratorFramework]
                           )
{
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val connectionOptions = connectionOpts
  private val curator: Option[CuratorFramework] = curatorOpt

  def this() = this(AuthOptions(), ZookeeperOptions(), ConnectionOptions(), None)

  def withAuthOptions(authOptions: AuthOptions) = new ClientBuilder(authOptions, zookeeperOptions, connectionOptions, curator)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) = new ClientBuilder(authOptions, zookeeperOptions, connectionOptions, curator)

  def withCuratorConnection(curator: CuratorFramework) = new ClientBuilder(authOptions, zookeeperOptions, connectionOptions, Some(curator))

  def withConnectionOptions(clientOptions: ConnectionOptions) = new ClientBuilder(authOptions, zookeeperOptions, clientOptions, curator)

  def build() = new Client(connectionOptions, authOptions, zookeeperOptions, curator)

  def getConnectionOptions = connectionOptions.copy()

  def getZookeeperOptions = zookeeperOptions.copy()

  def getAuthOptions = authOptions.copy()
}