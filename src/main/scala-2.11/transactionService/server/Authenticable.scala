package transactionService.server

trait Authenticable {
  val authClient: authService.ClientAuth
}
