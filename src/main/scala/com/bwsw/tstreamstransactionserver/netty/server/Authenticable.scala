package com.bwsw.tstreamstransactionserver.netty.server

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}
import com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthServiceImpl

trait Authenticable extends AuthServiceImpl{
  def authenticate[A](token: Int)(body: => A)(implicit context: ExecutionContext): ScalaFuture[A] = {
    if (isValid(token)) ScalaFuture(body) else ScalaFuture.failed(new TokenInvalidException)
  }
  def authenticateFutureBody[A](token: Int)(body: => ScalaFuture[A]): ScalaFuture[A] = {
    if (isValid(token)) body else ScalaFuture.failed(new TokenInvalidException)
  }
}
