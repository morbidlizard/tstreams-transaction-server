package transactionService

import com.twitter.finagle.Thrift
import com.twitter.util.Await
import transactionService.impl.{StreamServiceImpl, TransactionMetaServiceImpl}

object Server extends App {
  private class ThriftTransactionServer extends TransactionMetaServiceImpl

  private class ThriftStreamServer extends StreamServiceImpl

  val server = Thrift.server
  val iface1 = server.serveIface("localhost:8080", new ThriftStreamServer)


  val iface2 = server.serveIface("localhost:8081", new ThriftTransactionServer)

  Await.ready(iface1)
  Await.ready(iface2)
  //#thriftserverapi

}
