package com.bwsw.tstreamstransactionserver.netty.server.authService

import com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions
import com.google.common.cache.CacheBuilder


trait AuthServiceImpl {
  val authOpts: AuthOptions

  val random = scala.util.Random
  val usersToken = CacheBuilder.newBuilder()
    .maximumSize(authOpts.activeTokensNumber)
    .expireAfterAccess(authOpts.tokenTTL, java.util.concurrent.TimeUnit.SECONDS)
    .build[java.lang.Integer, String]()

  def authenticate(authKey: String): Int = {
    if (authKey == authOpts.key) {
      val token = random.nextInt(Integer.MAX_VALUE)
      usersToken.put(token, authKey)

      token
    } else -1
  }

  def isValid(token: Int): Boolean = token != -1 && usersToken.getIfPresent(token) != null
}