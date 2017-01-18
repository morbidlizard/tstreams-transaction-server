package netty.server

import com.sleepycat.je.Environment
import configProperties.ServerConfig
import netty.server.streamService.StreamServiceImpl
import netty.server.transactionDataService.TransactionDataServiceImpl
import netty.server.transactionMetaService.TransactionMetaServiceImpl
import netty.server.сonsumerService.ConsumerServiceImpl
import org.rocksdb.Options


class TransactionServer(override val config: ServerConfig = new configProperties.ServerConfig(new configProperties.ConfigFile("src/main/resources/serverProperties.properties")))
  //extends TransactionService[ScalaFuture]
  extends TransactionDataServiceImpl
    with TransactionMetaServiceImpl
    with ConsumerServiceImpl
    with StreamServiceImpl
{
  override val consumerEnvironment: Environment = transactionMetaEnviroment
  def close() = {
//    closeConsumerDatabase()
    closeTransactionDataDatabases()
//    closeTransactionMetaDatabases()
//    closeTransactionMetaEnviroment()
//    closeStreamEnviromentAndDatabase()
  }
}
