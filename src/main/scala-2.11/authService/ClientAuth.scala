package authService

import authService.rpc.AuthService
import com.twitter.finagle.{Failure, Thrift}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Monitor, Time, Future => TwitterFuture}

class ClientAuth(ipAddress: String) extends AuthService[TwitterFuture] {
  private val client = Thrift.client

  private def interface = {
    val interface= client.newServiceIface[AuthService.ServiceIface](ipAddress, "transaction")
    interface.copy(
      authenticate = interface.authenticate,
      isValid = interface.isValid
    )
  }

  private final val request = Thrift.client.newMethodIface(interface)
  override def authenticate(login: String, password: String): TwitterFuture[String] = request.authenticate(login,password)
  override def isValid(token: String): TwitterFuture[Boolean] = request.isValid(token)
}
