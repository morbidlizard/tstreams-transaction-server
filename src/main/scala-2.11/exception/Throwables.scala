package exception

import java.io.FileNotFoundException

object Throwables {
  val tokenInvalidExceptionMessage: String = "Token isn't valid"
  object TokenInvalidException extends IllegalArgumentException(tokenInvalidExceptionMessage)
  def tokenInvalidException: Throwable = throw TokenInvalidException

  val lockoutTransactionExceptionMessage: String ="com.sleepycat.je.LockTimeoutException"

  val StreamNotExistMessage: String = "Stream doesn't exist in database!"
  class StreamNotExist extends NoSuchElementException(StreamNotExistMessage)

  val configNotFoundMessage: String = "Config isn't found!"
  class ConfigNotFoundException extends FileNotFoundException(configNotFoundMessage)
}
