package transactionService.impl

trait Authenticable {
  val authClient: authService.ClientAuth
}
