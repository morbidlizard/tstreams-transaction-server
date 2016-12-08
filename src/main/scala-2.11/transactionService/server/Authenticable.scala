package transactionService.server

import authService.AuthClient
import com.twitter.util.{Future => TwitterFuture}


trait Authenticable {
  val authClient: AuthClient
  def authenticate[A](token: String)(body: => A) = authClient.isValid(token) flatMap(_ => TwitterFuture(body))
}
