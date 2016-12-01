package authService

import authService.impl.AuthServiceImpl
import com.twitter.finagle.Thrift
import com.twitter.util.{Await, Closable, Future, Time}

class AuthServer extends AuthServiceImpl with Closable {
  val start = Thrift.server.serveIface(configProperties.AuthConfig.authAddress, this)
  override def close(deadline: Time): Future[Unit] = start.close(deadline)
}



object AuthServer extends App {
  val server = new AuthServer
  Await.ready(server.start)
}
