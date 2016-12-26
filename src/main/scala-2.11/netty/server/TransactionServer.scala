package netty.server

import scala.concurrent.{Future => ScalaFuture}
import transactionService.rpc.TransactionService
import netty.server.streamService.StreamServiceImpl
import netty.server.transactionDataService.TransactionDataServiceImpl
import netty.server.transactionMetaService.TransactionMetaServiceImpl
import netty.server.сonsumerService.ConsumerServiceImpl


class TransactionServer(override val ttlToAdd: Int = configProperties.ServerConfig.transactionDataTtlAdd)
  extends TransactionService[ScalaFuture]
    with ConsumerServiceImpl
    with StreamServiceImpl
    with TransactionMetaServiceImpl
    with TransactionDataServiceImpl
