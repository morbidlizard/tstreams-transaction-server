
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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