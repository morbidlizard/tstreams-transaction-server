package transactionService

import com.twitter.finagle.Thrift
import com.twitter.util.Await
import transactionService.impl.{StreamServiceImpl, TransactionMetaServiceImpl}

object Server extends App {
  private class ThriftServer extends StreamServiceImpl

  val server = Thrift.server
  val iface1 = server.serveIface("localhost:8080", new ThriftServer)
  val iface2 = server.serveIface("localhost:8081", new ThriftServer)

  Await.ready(iface2)
  Await.ready(iface1.close())
  //#thriftserverapi

}
