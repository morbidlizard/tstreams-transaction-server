package authService

import authService.rpc.AuthService
import com.twitter.finagle.{Failure, Thrift}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Monitor, Time, Future => TwitterFuture}

class ClientAuth(ipAddress: String) extends AuthService[TwitterFuture] {
  private val client = Thrift.client.withMonitor(new Monitor {
    def handle(error: Throwable): Boolean = error match {
      case e: com.twitter.util.TimeoutException => true
      case e: Failure => {
        Logger.get().log(Level.ERROR, e.getMessage, e)
        true
      }
      case _ => false
    }
  })

  private val interface= client.newServiceIface[AuthService.ServiceIface](ipAddress, "transaction")
  private val interfaceCopy = interface.copy(
    authenticate = interface.authenticate,
    isValid = interface.isValid
  )
  private val request = Thrift.client.newMethodIface(interfaceCopy)

  override def authenticate(login: String, password: String): TwitterFuture[String] = request.authenticate(login,password)
  override def isValid(token: String): TwitterFuture[Boolean] = request.isValid(token)
}
