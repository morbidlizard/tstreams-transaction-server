package com.bwsw.tstreamstransactionserver.netty.client

import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import org.apache.curator.framework.CuratorFramework

class ClientProxy(clientOpts: ConnectionOptions,
                  authOpts: AuthOptions,
                  zookeeperOptions: ZookeeperOptions,
                  curatorConnection: Option[CuratorFramework] = None)
{



}
