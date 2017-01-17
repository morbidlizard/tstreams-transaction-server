package transactionService.server

import transactionService.server.authService.AuthServiceImpl
import exception.Throwables.tokenInvalidException
import com.twitter.util.{Future => TwitterFuture}

trait Authenticable extends AuthServiceImpl{
  def authenticate[A](token: Int)(body: => A): TwitterFuture[A] = {
    isValid(token) flatMap (isValid => if (isValid) TwitterFuture(body) else TwitterFuture.exception(tokenInvalidException))
  }
  def authenticateFutureBody[A](token: Int)(body: => TwitterFuture[A]): TwitterFuture[A] = {
    isValid(token) flatMap (isValid => if (isValid) body else TwitterFuture.exception(tokenInvalidException))
  }
}
