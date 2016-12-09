package transactionService.server.сonsumerService


import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.Context
import transactionService.server.сonsumerService.ConsumerServiceImpl._
import transactionService.server.{Authenticable, CheckpointTTL}
import transactionService.rpc.ConsumerService
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl


trait ConsumerServiceImpl extends ConsumerService[TwitterFuture]
  with Authenticable
  with CheckpointTTL
{
  def getConsumerState(token: String, name: String, stream: String, partition: Int): TwitterFuture[Long] =
    authenticate(token) {
      val streamNameToLong = getStream(name).streamNameToLong
      Option(consumerPrimaryIndex.get(new ConsumerKey(name, streamNameToLong, partition))) match {
        case Some(consumer) => consumer.transactionID
        case None => -1L
      }
    }

  def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): TwitterFuture[Boolean] =
    authenticate(token) {
      val streamNameToLong = getStream(name).streamNameToLong
      consumerPrimaryIndex.put(new ConsumerTransaction(name, streamNameToLong, partition, transaction))
      true
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
