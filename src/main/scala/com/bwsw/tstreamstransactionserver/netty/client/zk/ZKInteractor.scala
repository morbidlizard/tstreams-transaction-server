package com.bwsw.tstreamstransactionserver.netty.client.zk

import java.io.Closeable

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair

abstract class ZKInteractor(onMasterChangeDo: Either[Throwable, Option[SocketHostPortPair]] => Unit)
{
  def getCurrentMaster: Either[Throwable, Option[SocketHostPortPair]]
  def stop(): Unit
}
