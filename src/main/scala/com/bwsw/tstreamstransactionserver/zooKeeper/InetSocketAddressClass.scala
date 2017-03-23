package com.bwsw.tstreamstransactionserver.zooKeeper

case class InetSocketAddressClass(address: String, port: Int){
  override def toString: String = s"/$address:$port"
}