package transactionService

import com.twitter.finagle.Thrift
import com.twitter.util.Await
import org.apache.thrift.protocol.{TBinaryProtocol, TCompactProtocol}
import transactionService.impl.{ConsumerServiceImpl, StreamServiceImpl, TransactionDataServiceImpl, TransactionMetaServiceImpl}

object Server extends App {
  private class ThriftTransactionServer extends TransactionMetaServiceImpl
  private class ThriftStreamServer extends StreamServiceImpl
  private class ThriftDataServer extends TransactionDataServiceImpl
  private class ThriftConsumerServer extends ConsumerServiceImpl

  val server = Thrift.server

  val iface1 = server.serveIface("localhost:8080", new ThriftStreamServer)
  val iface2 = server.serveIface("localhost:8081", new ThriftTransactionServer)
  val iface3 = server.serveIface("localhost:8082", new ThriftDataServer)
  val iface4 = server.serveIface("localhost:8083", new ThriftConsumerServer)


  Await.ready(iface1)
  Await.ready(iface2)
  Await.ready(iface3)
  Await.ready(iface4)
  //#thriftserverapi

}
