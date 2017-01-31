package com.bwsw.tstreamstransactionserver.netty.server.authService

import com.bwsw.tstreamstransactionserver.configProperties.ServerConfig
import com.google.common.cache.CacheBuilder

import scala.concurrent.{Future => ScalaFuture}


trait AuthServiceImpl {
  val config: ServerConfig

  val random = scala.util.Random
  val usersToken = CacheBuilder.newBuilder()
    .maximumSize(config.authTokenActiveMax)
    .expireAfterAccess(config.authTokenTimeExpiration, java.util.concurrent.TimeUnit.SECONDS)
    .build[java.lang.Integer, String]()

  def authenticate(authKey: String): Int = {
    if (authKey == config.authKey) {
      val token = random.nextInt(Integer.MAX_VALUE)
      usersToken.put(token, authKey)

      token
    } else -1
  }

   def isValid(token: Int): Boolean = token != -1 && usersToken.getIfPresent(token) != null
}