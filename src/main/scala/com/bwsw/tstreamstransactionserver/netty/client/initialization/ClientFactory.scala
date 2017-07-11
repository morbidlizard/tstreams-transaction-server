package com.bwsw.tstreamstransactionserver.netty.client.initialization


import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import org.apache.curator.framework.CuratorFramework

trait ClientFactory {
//  def makeLocalClient(transactionServer: TransactionServer)
  def makeRemoteClient(clientOpts: ConnectionOptions,
                       authOpts: AuthOptions,
                       zookeeperOptions: ZookeeperOptions,
                       curatorConnection: Option[CuratorFramework] = None)
}
