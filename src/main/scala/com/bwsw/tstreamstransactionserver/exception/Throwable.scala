package com.bwsw.tstreamstransactionserver.exception

import java.net.SocketTimeoutException

object Throwable {
  val TokenInvalidExceptionMessage: String = "Token isn't valid."
  class TokenInvalidException extends IllegalArgumentException(TokenInvalidExceptionMessage)

  val serverConnectionExceptionMessage: String = "Can't connect to Server."
  class ServerConnectionException extends SocketTimeoutException(serverConnectionExceptionMessage)

  val serverUnreachableExceptionMessage: String = "Server is unreachable."
  class ServerUnreachableException(socket: String) extends SocketTimeoutException(s"Server $socket is unreachable.")

//  val requestTimeoutExceptionMessage: String = "Request exceeds timeout."
  class RequestTimeoutException(reqId: Int, ttl: Long) extends Exception(s"Request $reqId exceeds $ttl ms.")

  val zkGetMasterExceptionMessage: String = "Can't get master from ZooKeeper."
  class ZkGetMasterException(endpoints: String) extends Exception(s"Can't get master from ZooKeeper servers: $endpoints.")

  val zkNoConnectionExceptionMessage: String = "Can't connect to ZooKeeper server(s): "
  class ZkNoConnectionException(endpoints: String) extends Exception(new StringBuilder(zkNoConnectionExceptionMessage).append(endpoints).append('!').toString())

  class MethodDoesnotFoundException(method: String) extends IllegalArgumentException(new StringBuilder(method).append(" isn't implemented!").toString())

  class InvalidSocketAddress(message: String) extends IllegalArgumentException(message)

//  val StreamDoesntNotExistMessage: String = "Stream doesn't exist in database!"
  class StreamDoesNotExist(stream: String, isPartialMessage: Boolean = true) extends {
    val message = if (isPartialMessage) s"Stream $stream doesn't exist in database!" else stream
  } with NoSuchElementException(message)

  val PackageTooBigExceptionMessagePart: String = "A size of client request is greater"
  class PackageTooBigException(msg: String = "") extends Exception(msg)

  def byText(text: String): Throwable = text match {
    case TokenInvalidExceptionMessage => new TokenInvalidException
    case message if message.matches(s"Stream (.*) doesn't exist in database!") => new StreamDoesNotExist(message, isPartialMessage = false)
    case message if message.contains(PackageTooBigExceptionMessagePart) => new PackageTooBigException(text)
    case _ => new Exception(text)
  }
}
