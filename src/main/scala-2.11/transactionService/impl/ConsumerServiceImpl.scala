package transactionService.impl

import java.io.{Closeable, File}
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import scala.concurrent.{Future => ScalaFuture}
import com.twitter.util.{Future => TwitterFuture}
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import com.sleepycat.je.{DatabaseEntry, Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model._
import transactionService.rpc.ConsumerService
import transactionService.impl.`implicit`.Implicits._
import ConsumerServiceImpl._
import transactionService.Context

trait ConsumerServiceImpl extends ConsumerService[TwitterFuture]
  with Closeable
  with Authenticable
{


  def getConsumerState(token: String, name: String, stream: String, partition: Int): TwitterFuture[Long] = authClient.isValid(token) flatMap {isValid=>
    if (isValid) {
      implicit val context = Context.transactionContexts.getContext(partition, stream.toInt)
      val transaction = ScalaFuture {
        Option(consumerPrimaryIndex.get(new ConsumerKey(name, stream, partition))) match {
          case Some(consumer) => consumer.transactionID
          case None => -1L
        }
      }.as[TwitterFuture[Long]]

      transaction flatMap TwitterFuture.value
    } else TwitterFuture.exception(throw new IllegalArgumentException("Token isn't valid"))
  }

  def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): TwitterFuture[Boolean] = authClient.isValid(token) flatMap {isValid=>
    if (isValid) {
      implicit val context = transactionService.Context.transactionContexts.getContext(partition, stream.toInt)
      val isSetted = ScalaFuture {
        consumerPrimaryIndex.put(new ConsumerServiceImpl.ConsumerTransaction(name, stream, partition, transaction))
        true
      }.as[TwitterFuture[Boolean]]

      isSetted flatMap TwitterFuture.value
    } else TwitterFuture.exception(throw new IllegalArgumentException("Token isn't valid"))
  }

  override def close(): Unit = {
    entityStore.close()
    environment.close()
  }
}

private object ConsumerServiceImpl {
  final val storeName = "ConsumerStore"

  final val consumerKey = new DatabaseEntry("consumer")
  final val streamKey = new DatabaseEntry("stream")
  final val partitionKey = new DatabaseEntry("partition")
  final val txnKey = new DatabaseEntry("txn")

  val directory = TransactionMetaServiceImpl.directory
  val storeConfig = new StoreConfig()
    .setAllowCreate(true)
    .setTransactional(true)
  val environment = TransactionMetaServiceImpl.environment
  val entityStore = new EntityStore(environment, storeName, storeConfig)

  val consumerPrimaryIndex = entityStore.getPrimaryIndex(classOf[ConsumerKey], classOf[ConsumerServiceImpl.ConsumerTransaction])

  @Entity
  class ConsumerTransaction extends transactionService.rpc.ConsumerTransaction {
    @PrimaryKey private var key: ConsumerKey = _
    private var transactionIDDB: java.lang.Long = _

    def this(name: String,
             stream: String,
             partition: Int,
             transactionID: java.lang.Long
            ) {
      this()
      this.transactionIDDB = transactionID
      this.key = new ConsumerKey(name, stream, partition)
    }

    override def transactionID: Long = transactionIDDB
    override def name: String = key.name
    override def stream: String = key.stream
    override def partition: Int = key.partition
    override def toString: String = s"Consumer transaction: ${key.toString}"
  }

  @Persistent
  class ConsumerKey {
    @KeyField(1) var name: String = _
    @KeyField(2) var stream: String = _
    @KeyField(3) var partition: Int = _
    def this(name: String, stream: String, partition:Int) = {
      this()
      this.name = name
      this.stream = stream
      this.partition = partition
    }

    override def toString: String = s"consumer:$name\tstream:$stream\tpartition:$partition\t"
  }
}
