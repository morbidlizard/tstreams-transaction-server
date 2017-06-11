package com.bwsw.tstreamstransactionserver.netty.server.authService

import com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions
import com.google.common.cache.CacheBuilder
import org.slf4j.{Logger, LoggerFactory}
import scala.util.Random

class AuthServiceImpl(authOpts: AuthOptions) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val usersToken = CacheBuilder.newBuilder()
    .maximumSize(authOpts.activeTokensNumber)
    .expireAfterAccess(authOpts.tokenTTL, java.util.concurrent.TimeUnit.SECONDS)
    .build[java.lang.Integer, String]()

  private[server] final def authenticate(authKey: String): Int = {
    if (authKey == authOpts.key) {
      val token = Random.nextInt(Integer.MAX_VALUE)
      usersToken.put(token, authKey)
      if (logger.isDebugEnabled()) logger.debug(s"Client with authkey $authKey is successfully authenticated and assigned token $token.")
      token
    } else {
      if (logger.isDebugEnabled()) logger.debug(s"Client with authkey $authKey isn't authenticated and assigned token -1.")
      -1
    }
  }

  private[server] final def isValid(token: Int): Boolean = {
    val isValid = token != -1 && usersToken.getIfPresent(token) != null

    if (isValid)
      logger.info(s"Client token $token is accepted.")
    else
      logger.warn(s"Client token $token is expired or doesn't exist.")

    isValid
  }
}