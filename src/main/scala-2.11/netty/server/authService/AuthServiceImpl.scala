package netty.server.authService


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future => ScalaFuture}
import com.google.common.cache.CacheBuilder
import transactionService.rpc.AuthService


trait AuthServiceImpl extends AuthService[ScalaFuture] {

  val random = scala.util.Random
  val usersToken = CacheBuilder.newBuilder()
    .maximumSize(configProperties.ServerConfig.authTokenActiveMax)
    .expireAfterAccess(configProperties.ServerConfig.authTokenTimeExpiration, java.util.concurrent.TimeUnit.SECONDS)
    .build[java.lang.Integer, (String,String)]()


  override def authenticate(login: String, password: String): ScalaFuture[Int] = ScalaFuture.successful{
    val token = random.nextInt()
    usersToken.put(token, (login, password))
    token
  }

  override def isValid(token: Int): ScalaFuture[Boolean] = ScalaFuture.successful(usersToken.getIfPresent(token) != null)
}
