package com.bwsw.netty.server

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}
import com.bwsw.exception.Throwables.tokenInvalidException
import com.bwsw.netty.server.authService.AuthServiceImpl

trait Authenticable extends AuthServiceImpl{
  def authenticate[A](token: Int)(body: => A)(implicit context: ExecutionContext): ScalaFuture[A] = {
    if (isValid(token)) ScalaFuture(body) else ScalaFuture.failed(tokenInvalidException)
  }
  def authenticateFutureBody[A](token: Int)(body: => ScalaFuture[A]): ScalaFuture[A] = {
    if (isValid(token)) body else ScalaFuture.failed(tokenInvalidException)
  }
}
