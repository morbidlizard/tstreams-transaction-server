package authService.impl

import authService.rpc.AuthService
import pdi.jwt.{Jwt, JwtAlgorithm, JwtHeader, JwtClaim}
import com.twitter.util.Future
import AuthServiceImpl._

//TODO remake to a stateful service using hashMap
trait AuthServiceImpl extends AuthService[Future] {
  override def authenticate(login: String, password: String): Future[String] = Future(
   Jwt.encode(
     JwtHeader(headerEncoderAlgorithm),
     JwtClaim(s"""{"user":$login,"password":$password}""").issuedNow.expiresIn(tokenExpirationTimeInSeconds),
     secretKey
    )
  )

  override def isValid(token: String): Future[Boolean] = Future (
   Jwt.isValid(token, secretKey, Seq(JwtAlgorithm.HMD5))
  )
}

private object AuthServiceImpl {
  val tokenExpirationTimeInSeconds: Long = configProperties.AuthConfig.authTokenTimeExpiration
  val secretKey: String = "secretKey"
  val headerEncoderAlgorithm: JwtAlgorithm = JwtAlgorithm.HMD5
}
