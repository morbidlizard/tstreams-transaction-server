package com.bwsw.tstreamstransactionserver.netty

import com.google.common.net.InetAddresses

case class InetSocketAddressClass(address: String, port: Int){
  override def toString: String = s"/$address:$port"
}
object InetSocketAddressClass {
  def isValidSocketAddress(inetAddress: String, port: Int): Boolean = {
    val validHostname = scala.util.Try(java.net.InetAddress.getByName(inetAddress))
    if (
      port.toInt > 0 &&
        port.toInt < 65536 &&
        inetAddress != null &&
        (InetAddresses.isInetAddress(inetAddress) || validHostname.isSuccess)
    ) true
    else false
  }
}