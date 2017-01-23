package netty.server

import com.sleepycat.je.{Environment, Transaction}
import configProperties.ServerConfig
import netty.server.streamService.StreamServiceImpl
import netty.server.transactionDataService.TransactionDataServiceImpl
import netty.server.transactionMetaService.TransactionMetaServiceImpl
import netty.server.сonsumerService.ConsumerServiceImpl
import org.rocksdb.Options
import transactionService.rpc.ConsumerTransaction


class TransactionServer(override val config: ServerConfig)
  //extends TransactionService[ScalaFuture]
  extends TransactionDataServiceImpl
    with TransactionMetaServiceImpl
    with ConsumerServiceImpl
    with StreamServiceImpl
{
  override val consumerEnvironment: Environment = transactionMetaEnviroment

  override def putConsumerTransaction(databaseTxn: Transaction, txn: ConsumerTransaction): Boolean = {
    setConsumerState(databaseTxn, txn.name, txn.stream, txn.partition, txn.transactionID)
  }
  def close() = {
    //closeConsumerDatabase()
    closeTransactionDataDatabases()
    //closeTransactionMetaDatabases()
    //closeTransactionMetaEnviroment()
    //closeStreamEnviromentAndDatabase()
  }
}
