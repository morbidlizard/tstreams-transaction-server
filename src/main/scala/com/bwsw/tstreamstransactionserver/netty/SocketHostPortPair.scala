package com.bwsw.tstreamstransactionserver.netty

import com.bwsw.tstreamstransactionserver.exception.Throwable.InvalidSocketAddress
import com.google.common.net.InetAddresses

import scala.util.{Failure, Success, Try}

case class SocketHostPortPair(address: String, port: Int){
  override def toString: String = s"/$address:$port"
  def get = s"$address:$port"
}

object SocketHostPortPair {

  def fromString(hostPortPair: String): Option[SocketHostPortPair] = {
    val splitIndex = hostPortPair.lastIndexOf(':')
    if (splitIndex == -1)
      None
    else {
      val (address, port) = hostPortPair.splitAt(splitIndex)
      scala.util.Try(port.tail.toInt) match {
        case Success(port) =>
          Try(create(address, port)) match {
          case Success(socketHostPortPair) => Some(socketHostPortPair)
          case Failure(_) => None
        }
        case Failure(_) => None
      }
    }
  }

  def isValid(inetAddress: String, port: Int): Boolean = {
    val validHostname = scala.util.Try(java.net.InetAddress.getByName(inetAddress))
    if (port > 0 && port < 65536 && inetAddress != null &&
        (InetAddresses.isInetAddress(inetAddress) || validHostname.isSuccess)
    ) true
    else false
  }

  def create(inetAddress: String, port: Int) = {
    if(!isValid(inetAddress, port))
      throw new InvalidSocketAddress(s"Address $inetAddress:$port is not a correct socket address pair.")
    SocketHostPortPair(inetAddress, port)
  }

  def validate(inetAddress: String, port: Int) = create(inetAddress: String, port: Int)
}