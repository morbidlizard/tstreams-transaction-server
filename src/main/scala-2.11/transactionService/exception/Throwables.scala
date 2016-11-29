package transactionService.exception

object Throwables {
  val tokenInvalidExceptionMessage: String = "Token isn't valid"
  def tokenInvalidException: Throwable = throw new IllegalArgumentException(tokenInvalidExceptionMessage)

  val StreamNotExistMessage: String = "Stream doesn't exist in database!"
  class StreamNotExist extends NoSuchElementException(StreamNotExistMessage)
}
