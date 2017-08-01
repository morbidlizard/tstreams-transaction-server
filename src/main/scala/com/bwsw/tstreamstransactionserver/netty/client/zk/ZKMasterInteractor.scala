package com.bwsw.tstreamstransactionserver.netty.client.zk

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState

class ZKMasterInteractor(connection: CuratorFramework,
                         masterPrefix: String,
                         onMasterChangeDo: Either[Throwable, Option[SocketHostPortPair]] => Unit,
                         onConnectionStateChangeDo: ConnectionState => Unit)
  extends ZKInteractor(onMasterChangeDo) {
  private val (listener, zkMasterPathMonitor) = {
    val listener =
      ZKClient.addConnectionListener(
        connection,
        onConnectionStateChangeDo
      )

    val monitor =
      new ZKMasterPathMonitor(
        connection,
        masterPrefix,
        newMaster => {
          master = newMaster
          onMasterChangeDo(master)
        }
      )

    monitor
      .startMonitoringMasterServerPath()

    (listener, monitor)
  }
  @volatile private var master: Either[Throwable, Option[SocketHostPortPair]] =
    Right(None)

  def getCurrentMaster: Either[Throwable, Option[SocketHostPortPair]] = {
    master
  }

  override def stop(): Unit = {
    zkMasterPathMonitor
      .stopMonitoringMasterServerPath()

    ZKClient
      .removeConnectionListener(
        connection,
        listener
      )
  }
}
