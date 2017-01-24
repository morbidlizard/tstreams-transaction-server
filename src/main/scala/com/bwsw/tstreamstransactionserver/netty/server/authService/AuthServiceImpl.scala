package com.bwsw.tstreamstransactionserver.netty.server.authService

import scala.concurrent.{Future => ScalaFuture}
import com.google.common.cache.CacheBuilder
import com.bwsw.tstreamstransactionserver.configProperties.ServerConfig


trait AuthServiceImpl {
  val config: ServerConfig

  val random = scala.util.Random
  val usersToken = CacheBuilder.newBuilder()
    .maximumSize(config.authTokenActiveMax)
    .expireAfterAccess(config.authTokenTimeExpiration, java.util.concurrent.TimeUnit.SECONDS)
    .build[java.lang.Integer, (String,String)]()

  def authenticate(login: String, password: String): Int = {
    val token = random.nextInt()
    usersToken.put(token, (login, password))
    token
  }

   def isValid(token: Int): Boolean = usersToken.getIfPresent(token) != null
}