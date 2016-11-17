package transactionService.server

import com.twitter.finagle.Thrift
import com.twitter.util.{Await, Future}
import transactionService.server.transactionDataService.TransactionDataServiceImpl
import transactionService.server.streamService.StreamServiceImpl
import transactionService.server.сonsumerService.ConsumerServiceImpl
import transactionService.rpc.TransactionService
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl

class TransactionServiceImpl(override val authClient: authService.ClientAuth)
  extends TransactionService[Future]
    with ConsumerServiceImpl
    with StreamServiceImpl
    with TransactionMetaServiceImpl
    with TransactionDataServiceImpl

object TransactionServiceImpl extends App{

  val server = Thrift.server

  Await.ready(server.serveIface("localhost:8080", new TransactionServiceImpl(new authService.ClientAuth(":8081"))))
  //#thriftserverapi
}
