package authService

import authService.impl.AuthServiceImpl
import com.twitter.finagle.Thrift
import com.twitter.util.Await

object ServerAuth extends App {
  private class ThriftServer extends AuthServiceImpl
  val server = Thrift.server
  val iface = server.serveIface("localhost:8081", new ThriftServer)
  Await.ready(iface)
}
