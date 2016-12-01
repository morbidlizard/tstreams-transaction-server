package authService

import authService.impl.AuthServiceImpl
import com.twitter.finagle.Thrift
import com.twitter.util.Await

object AuthServer extends App {
  private class ThriftServer extends AuthServiceImpl
  val server = Thrift.server
  val iface = server.serveIface(configProperties.AuthConfig.authAddress, new ThriftServer)
  Await.ready(iface)
}
