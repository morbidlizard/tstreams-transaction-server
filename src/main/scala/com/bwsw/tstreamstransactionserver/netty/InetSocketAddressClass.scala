package com.bwsw.tstreamstransactionserver.netty

case class InetSocketAddressClass(address: String, port: Int){
  override def toString: String = s"/$address:$port"
}