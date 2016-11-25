package transactionService.exception

object Throwables {
  val tokenInvalidExceptionMessage: String = "Token isn't valid"
  def tokenInvalidException: Throwable = throw new IllegalArgumentException(tokenInvalidExceptionMessage)
}
