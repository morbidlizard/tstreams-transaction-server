package transactionService.impl

import com.twitter.finagle.Thrift
import com.twitter.util.{Await, Future}
import transactionService.rpc.TransactionService

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
