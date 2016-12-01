package transactionService.server

import authService.AuthClient
import com.twitter.util.{Future => TwitterFuture}
import transactionService.server.transactionDataService.TransactionDataServiceImpl
import transactionService.server.streamService.StreamServiceImpl
import transactionService.server.сonsumerService.ConsumerServiceImpl
import transactionService.rpc.TransactionService
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl


class TransactionServer(override val authClient: AuthClient, override val ttlToAdd: Int)
  extends TransactionService[TwitterFuture]
    with ConsumerServiceImpl
    with StreamServiceImpl
    with TransactionMetaServiceImpl
    with TransactionDataServiceImpl
