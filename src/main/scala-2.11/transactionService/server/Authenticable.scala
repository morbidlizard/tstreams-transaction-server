package transactionService.server

import authService.impl.AuthServiceImpl

trait Authenticable extends AuthServiceImpl{
  def authenticate[A](token: String)(body: => A) = isValid(token) map (_ => body)
}
