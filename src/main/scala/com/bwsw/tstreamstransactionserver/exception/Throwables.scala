package com.bwsw.tstreamstransactionserver.exception

import java.io.FileNotFoundException
import java.net.SocketTimeoutException

object Throwables {
  val tokenInvalidExceptionMessage: String = "Token isn't valid."
  class TokenInvalidException extends IllegalArgumentException(tokenInvalidExceptionMessage)

  val serverConnectionExceptionMessage: String = "Can't connect to Server."
  class ServerConnectionException extends SocketTimeoutException(serverConnectionExceptionMessage)

  val serverUnreachableExceptionMessage: String = "Server is unreachable."
  class ServerUnreachableException extends SocketTimeoutException(serverUnreachableExceptionMessage)

  val zkGetMasterExceptionMessage: String = "Can't get master from ZooKeeper."
  class ZkGetMasterException extends Exception(zkGetMasterExceptionMessage)

  val StreamDoesntNotExistMessage: String = "Stream doesn't exist in database!"
  class StreamDoesNotExist extends NoSuchElementException(StreamDoesntNotExistMessage)


  def byText(text: String) : Throwable = text match {
    case `tokenInvalidExceptionMessage` => new TokenInvalidException
    case StreamDoesntNotExistMessage =>  new StreamDoesNotExist
    case _ => new Exception(text)
  }
}
