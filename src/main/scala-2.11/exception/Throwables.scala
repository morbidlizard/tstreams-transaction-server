package exception

import java.io.FileNotFoundException

object Throwables {
  val tokenInvalidExceptionMessage: String = "Token isn't valid"
  class TokenInvalidException extends IllegalArgumentException(tokenInvalidExceptionMessage)
  def tokenInvalidException: Throwable = new TokenInvalidException

  val lockoutTransactionExceptionMessage: String ="com.sleepycat.je.LockTimeoutException"

  val StreamNotExistMessage: String = "Stream doesn't exist in database!"
  class StreamNotExist extends NoSuchElementException(StreamNotExistMessage)

  val configNotFoundMessage: String = "Config isn't found!"
  class ConfigNotFoundException extends FileNotFoundException(configNotFoundMessage)


  def byText(text: String) : Throwable = text match {
    case `tokenInvalidExceptionMessage` => new TokenInvalidException
    case `StreamNotExistMessage` =>  new StreamNotExist
    case _ => new Exception(text)
  }
}
