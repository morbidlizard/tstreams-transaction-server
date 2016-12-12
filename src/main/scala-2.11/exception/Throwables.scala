package exception

import java.io.FileNotFoundException

object Throwables {
  val tokenInvalidExceptionMessage: String = "Token isn't valid"
  def tokenInvalidException: Throwable = throw new IllegalArgumentException(tokenInvalidExceptionMessage)

  val lockoutTransactionExceptionMessage: String ="com.sleepycat.je.LockTimeoutException"

  val StreamNotExistMessage: String = "Stream doesn't exist in database!"
  class StreamNotExist extends NoSuchElementException(StreamNotExistMessage)

  val configNotFoundMessage: String = "Config isn't found!"
  class ConfigNotFoundException extends FileNotFoundException(configNotFoundMessage)
}
