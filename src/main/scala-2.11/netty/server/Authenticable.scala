package netty.server

import scala.concurrent.{Future => ScalaFuture}
import scala.concurrent.ExecutionContext.Implicits.global
import exception.Throwables.tokenInvalidException
import netty.server.authService.AuthServiceImpl

trait Authenticable extends AuthServiceImpl{
  def authenticate[A](token: Int)(body: => A): ScalaFuture[A] = {
    isValid(token) flatMap (isValid => if (isValid) ScalaFuture(body) else ScalaFuture.failed(tokenInvalidException))
  }
  def authenticateFutureBody[A](token: Int)(body: => ScalaFuture[A]): ScalaFuture[A] = {
    isValid(token) flatMap (isValid => if (isValid) body else ScalaFuture.failed(tokenInvalidException))
  }
}
