package authService

import authService.impl.AuthServiceImpl
import com.twitter.finagle.Thrift
import com.twitter.util.Await

object Server extends App {

  private class ThriftServer extends AuthServiceImpl

  val server = Thrift.server
  val iface1 = server.serveIface("localhost:8080", new ThriftServer)
  val iface2 = server.serveIface("localhost:8081", new ThriftServer)

  Await.ready(iface2)
  Await.ready(iface1.close())
}
