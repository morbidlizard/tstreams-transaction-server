package com.bwsw.tstreamstransactionserver.netty.client.zk

import java.io.Closeable

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.retry.RetryForever

class ZKMiddleware(externalZkClient: Option[CuratorFramework],
                   zookeeperOptions: ZookeeperOptions,
                   onMasterChangeDo: Either[Throwable, Option[SocketHostPortPair]] => Unit,
                   onConnectionStateChangeDo: => Either[Throwable, Option[SocketHostPortPair]] => ConnectionState => Unit)
  extends ZKInteractor(onMasterChangeDo)
    with Closeable {
  @volatile private var master: Either[Throwable, Option[SocketHostPortPair]] =
    Right(None)

  private val isExternalZkClient = externalZkClient.isDefined

  private val (zkClient, listener, zkMasterPathMonitor) = externalZkClient match {
    case Some(connection) =>
      val listener =
        ZKClient.addConnectionListener(
          connection,
          onConnectionStateChangeDo(master)
        )

      val monitor =
        new ZKMasterPathMonitor(
          connection,
          zookeeperOptions.prefix,
          newMaster => {
            master = newMaster
            onMasterChangeDo(master)
          }
        )

      monitor
        .startMonitoringMasterServerPath()

      (connection, listener, monitor)

    case None =>
      val zKLeaderClient = new ZKClient(
        zookeeperOptions.endpoints,
        zookeeperOptions.sessionTimeoutMs,
        zookeeperOptions.connectionTimeoutMs,
        new RetryForever(zookeeperOptions.retryDelayMs),
        zookeeperOptions.prefix,
        onConnectionStateChangeDo(master)
      )

      val monitor =
        new ZKMasterPathMonitor(
          zKLeaderClient.client,
          zookeeperOptions.prefix,
          newMaster => {
            master = newMaster
            onMasterChangeDo(master)
          }
        )

      monitor
        .startMonitoringMasterServerPath()

      (zKLeaderClient.client, zKLeaderClient.listener, monitor)
  }

  def getCurrentMaster: Either[Throwable, Option[SocketHostPortPair]] = {
    master
  }

  def setCurrentMaster(master: Either[Throwable, Option[SocketHostPortPair]]): Unit = {
    this.master = master
    onMasterChangeDo(this.master)
  }

  override def close(): Unit = {
    zkMasterPathMonitor
      .stopMonitoringMasterServerPath()

    ZKClient
      .removeConnectionListener(
        zkClient,
        listener
      )

    if (!isExternalZkClient) {
      zkClient.close()
    }
  }
}
