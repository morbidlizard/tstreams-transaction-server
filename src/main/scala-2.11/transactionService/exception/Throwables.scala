package transactionService.exception

object Throwables {
  val tokenInvalidException: Throwable = throw new IllegalArgumentException("Token isn't valid")
}
