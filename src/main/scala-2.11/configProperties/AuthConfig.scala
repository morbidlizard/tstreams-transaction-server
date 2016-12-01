package configProperties

import configProperties.Config._

object AuthConfig {
  private val config = new Config("src/main/resources/authProperties.properties")

  val authAddress = config.readProperty[String]("auth.address")

  val authTokenTimeExpiration = config.readProperty[Long]("auth.token.time.expiration")
}
