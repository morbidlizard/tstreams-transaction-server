package authService

import com.twitter.finagle.{Failure, Thrift}
import authService.rpc.AuthService
import authService.rpc.AuthService.ServiceIface
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Monitor}

object Client extends App  {
  val client = Thrift.client.withMonitor(new Monitor {
    def handle(error: Throwable): Boolean = error match {
      case e: com.twitter.util.TimeoutException => true
      case e: Failure => {
        Logger.get().log(Level.ERROR, e.getMessage, e)
        true
      }
      case _ => false
    }
  })

  val iface: ServiceIface = client.newServiceIface[AuthService.ServiceIface]("localhost:8080", "auth")
  val authCopy = iface.copy(
    authenticate = iface.authenticate,
    authorize = iface.authorize
  )
  val request = Thrift.client.newMethodIface(authCopy)

  val name = "Aleksandr"
  val password = "1488"

  val session = request.authenticate(name, password)
  val expiredSession = session flatMap {str=> request.authorize(str)}

  println(Await.ready(expiredSession))

}
