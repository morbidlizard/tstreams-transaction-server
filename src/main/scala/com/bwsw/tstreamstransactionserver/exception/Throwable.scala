package com.bwsw.tstreamstransactionserver.exception

import java.io.FileNotFoundException
import java.net.SocketTimeoutException

object Throwable {
  val tokenInvalidExceptionMessage: String = "Token isn't valid."
  class TokenInvalidException extends IllegalArgumentException(tokenInvalidExceptionMessage)

  val serverConnectionExceptionMessage: String = "Can't connect to Server."
  class ServerConnectionException extends SocketTimeoutException(serverConnectionExceptionMessage)

  val serverUnreachableExceptionMessage: String = "Server is unreachable."
  class ServerUnreachableException extends SocketTimeoutException(serverUnreachableExceptionMessage)

  class RequestTimeoutException(reqId: Int, ttl: Long) extends Exception(s"Request $reqId exceeds $ttl ms.")

  val zkGetMasterExceptionMessage: String = "Can't get master from ZooKeeper."
  class ZkGetMasterException extends Exception(zkGetMasterExceptionMessage)

  val zkNoConnectionExceptionMessage: String = "Can't connect to ZooKeeper server(s): "
  class ZkNoConnectionException(endpoints: String) extends Exception(new StringBuilder(zkNoConnectionExceptionMessage).append(endpoints).append('!').toString())

  class MethodDoesnotFoundException(method: String) extends IllegalArgumentException(new StringBuilder(method).append(" isn't implemented!").toString())

  val StreamDoesntNotExistMessage: String = "Stream doesn't exist in database!"
  class StreamDoesNotExist extends NoSuchElementException(StreamDoesntNotExistMessage)


  def byText(text: String) : Throwable = text match {
    case `tokenInvalidExceptionMessage` => new TokenInvalidException
    case StreamDoesntNotExistMessage =>  new StreamDoesNotExist
    case _ => new Exception(text)
  }
}
