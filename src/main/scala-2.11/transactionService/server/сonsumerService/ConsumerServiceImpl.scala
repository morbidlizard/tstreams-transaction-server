package transactionService.server.сonsumerService

import java.io.Closeable

import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import com.twitter.util.{Future => TwitterFuture}
import transactionService.Context
import exception.Throwables._
import transactionService.server.сonsumerService.ConsumerServiceImpl._
import transactionService.server.Authenticable
import transactionService.rpc.ConsumerService
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl

import scala.concurrent.{Future => ScalaFuture}

trait ConsumerServiceImpl extends ConsumerService[TwitterFuture]
  with Authenticable
{


  def getConsumerState(token: String, name: String, stream: String, partition: Int): TwitterFuture[Long] = authClient.isValid(token) flatMap {isValid=>
    if (isValid) {
      implicit val context = Context.transactionContexts.getContext(partition, stream.toInt)
      ScalaFuture {
        Option(consumerPrimaryIndex.get(new ConsumerKey(name, stream, partition))) match {
          case Some(consumer) => consumer.transactionID
          case None => -1L
        }
      }.as[TwitterFuture[Long]]
    } else TwitterFuture.exception(tokenInvalidException)
  }

  def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): TwitterFuture[Boolean] = authClient.isValid(token) flatMap {isValid=>
    if (isValid) {
      implicit val context = Context.transactionContexts.getContext(partition, stream.toInt)
      ScalaFuture {
        consumerPrimaryIndex.put(new ConsumerTransaction(name, stream, partition, transaction))
        true
      }.as[TwitterFuture[Boolean]]
    } else TwitterFuture.exception(tokenInvalidException)
  }
}

object ConsumerServiceImpl {
  val storeName = configProperties.DB.ConsumerStoreName

  val directory   =  TransactionMetaServiceImpl.directory
  val storeConfig = new StoreConfig()
    .setAllowCreate(true)
    .setTransactional(true)
  val environment = TransactionMetaServiceImpl.environment
  val entityStore = new EntityStore(environment, storeName, storeConfig)

  val consumerPrimaryIndex = entityStore.getPrimaryIndex(classOf[ConsumerKey], classOf[ConsumerTransaction])

  def close(): Unit = {
    entityStore.close()
    environment.close()
  }
}
