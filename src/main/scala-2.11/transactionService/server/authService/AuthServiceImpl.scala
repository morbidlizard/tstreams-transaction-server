package transactionService.server.authService

import com.twitter.util.{Future => TwitterFuture}
import com.google.common.cache.CacheBuilder
import transactionService.rpc.AuthService


trait AuthServiceImpl extends AuthService[TwitterFuture] {

  val random = scala.util.Random
  val usersToken = CacheBuilder.newBuilder()
    .maximumSize(configProperties.AuthConfig.authTokenActiveMax)
    .expireAfterAccess(configProperties.AuthConfig.authTokenTimeExpiration, java.util.concurrent.TimeUnit.SECONDS)
    .build[java.lang.Integer, (String,String)]()


  override def authenticate(login: String, password: String): TwitterFuture[Int] = TwitterFuture {
    val token = random.nextInt()
    usersToken.put(token, (login, password))
    token
  }

  override def isValid(token: Int): TwitterFuture[Boolean] = TwitterFuture(usersToken.getIfPresent(token) != null)
}
