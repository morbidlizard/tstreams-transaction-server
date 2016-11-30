package transactionService.server

import authService.AuthClient

trait Authenticable {
  val authClient: AuthClient
}
