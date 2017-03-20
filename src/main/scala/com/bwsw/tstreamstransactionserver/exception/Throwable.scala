package com.bwsw.tstreamstransactionserver.exception

import java.net.SocketTimeoutException

object Throwable {
  val TokenInvalidExceptionMessage: String = "Token isn't valid."
  class TokenInvalidException extends IllegalArgumentException(TokenInvalidExceptionMessage)

  val serverConnectionExceptionMessage: String = "Can't connect to Server."
  class ServerConnectionException extends SocketTimeoutException(serverConnectionExceptionMessage)

  val serverUnreachableExceptionMessage: String = "Server is unreachable."
  class ServerUnreachableException extends SocketTimeoutException(serverUnreachableExceptionMessage)

  val requestTimeoutExceptionMessage: String = "Request exceeds timeout."
  class RequestTimeoutException(reqId: Int, ttl: Long) extends Exception(s"Request $reqId exceeds $ttl ms.")

  val zkGetMasterExceptionMessage: String = "Can't get master from ZooKeeper."
  class ZkGetMasterException extends Exception(zkGetMasterExceptionMessage)

  val zkNoConnectionExceptionMessage: String = "Can't connect to ZooKeeper server(s): "
  class ZkNoConnectionException(endpoints: String) extends Exception(new StringBuilder(zkNoConnectionExceptionMessage).append(endpoints).append('!').toString())

  class MethodDoesnotFoundException(method: String) extends IllegalArgumentException(new StringBuilder(method).append(" isn't implemented!").toString())

  class InvalidSocketAddress(message: String) extends IllegalArgumentException(message)

  val StreamDoesntNotExistMessage: String = "StreamWithoutKey doesn't exist in database!"
  class StreamDoesNotExist extends NoSuchElementException(StreamDoesntNotExistMessage)

  val PackageTooBigExceptionMessagePart: String = "A size of client request is greater"
  class PackageTooBigException(msg: String = "") extends Exception(msg)

  def byText(text: String): Throwable = text match {
    case TokenInvalidExceptionMessage => new TokenInvalidException
    case StreamDoesntNotExistMessage => new StreamDoesNotExist
    case message if message.contains(PackageTooBigExceptionMessagePart) => new PackageTooBigException(text)
    case _ => new Exception(text)
  }
}
