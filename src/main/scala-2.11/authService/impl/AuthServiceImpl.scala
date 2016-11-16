package authService.impl

import authService.rpc.AuthService
import pdi.jwt.{Jwt, JwtAlgorithm, JwtHeader, JwtClaim}
import com.twitter.util.Future
import AuthServiceImpl._

//TODO remake to stateful service, using hashMap
trait AuthServiceImpl extends AuthService[Future] {
  override def authenticate(login: String, password: String): Future[String] = Future(
   Jwt.encode(
     JwtHeader(headerEncoderAlgorithm),
     JwtClaim(s"""{"user":$login,"password":$password}""").issuedNow.expiresIn(tokenExpirationTimeInSeconds),
     secretKey
    )
  )

  override def isValid(token: String): Future[Boolean] = Future {
    val before = System.nanoTime()
    val res = Jwt.isValid(token, secretKey, Seq(JwtAlgorithm.HMD5))
    val after = System.nanoTime()
    println(after - before)
    res
  }
}

private object AuthServiceImpl {
  val tokenExpirationTimeInSeconds: Long = 120L
  val secretKey: String = "secretKey"
  val headerEncoderAlgorithm: JwtAlgorithm = JwtAlgorithm.HMD5
}
