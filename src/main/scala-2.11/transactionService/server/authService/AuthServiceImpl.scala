package transactionService.server.authService

import authService.rpc.AuthService
import com.twitter.util.{Future => TwitterFuture}
import com.google.common.cache.CacheBuilder
import shared.FNV


trait AuthServiceImpl extends AuthService[TwitterFuture] {

  val usersToken = CacheBuilder.newBuilder()
    .maximumSize(configProperties.AuthConfig.authTokenActiveMax)
    .expireAfterWrite(configProperties.AuthConfig.authTokenTimeExpiration, java.util.concurrent.TimeUnit.SECONDS)
    .softValues()
    .build[String,(String,String)]()


  override def authenticate(login: String, password: String): TwitterFuture[String] = TwitterFuture {
    val token = FNV.hash32a(s"$login$password".getBytes).toString
    usersToken.put(token, (login, password))
    token
  }

  override def isValid(token: String): TwitterFuture[Boolean] = TwitterFuture {
    if (token == null) false
    else usersToken.getIfPresent(token) != null
  }
}
