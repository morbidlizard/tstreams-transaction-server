package transactionService.server.transactionMetaService


import java.util.concurrent.TimeUnit

import com.sleepycat.je.{Environment, EnvironmentConfig, Put, WriteOptions}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.Context
import transactionService.server.{Authenticable, CheckpointTTL}
import transactionService.rpc._
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
import transactionService.server.сonsumerService.ConsumerServiceImpl.consumerPrimaryIndex
import transactionService.exception.Throwables._
import transactionService.rpc.TransactionStates.Checkpointed

import scala.concurrent.{Future => ScalaFuture}


trait TransactionMetaServiceImpl extends TransactionMetaService[TwitterFuture]
  with Authenticable
  with CheckpointTTL
{
  val logger = Logger.get(this.getClass)

  private val putType = Put.OVERWRITE
  private def checkTTL(ttl: Int) = {
    val ttlInHours = java.util.concurrent.TimeUnit.MILLISECONDS.toHours(ttl.toLong).toInt
    if (ttlInHours == 0) 1 else ttlInHours
  }

  def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      val transactionDB = environment.beginTransaction(null, null)
      val (producerTransactionOpt, consumerTransactionOpt) = (transaction.producerTransaction, transaction.consumerTransaction)

      val result = (producerTransactionOpt, consumerTransactionOpt) match {
        case (Some(txn), _) => getStreamTTL(txn.stream) flatMap { ttl =>
          implicit val context = Context.transactionContexts.getContext(txn.partition, txn.stream.hashCode)
          ScalaFuture {
            val writeOptions = if (txn.state == Checkpointed) new WriteOptions().setTTL(checkTTL(ttl)) else new WriteOptions()
            val isNotExist =
              producerPrimaryIndex
                .put(transactionDB, new ProducerTransaction(txn.transactionID, txn.state, txn.stream, txn.timestamp, txn.quantity, txn.partition), putType, writeOptions) != null
            if (isNotExist) {
              logger.log(Level.INFO, s"${txn.toString} inserted/updated!")
              isNotExist
            } else {
              logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
              isNotExist
            }
          }.as[TwitterFuture[Boolean]]
        }
        case (_, Some(txn)) =>
          import transactionService.server.сonsumerService._
          implicit val context = transactionService.Context.transactionContexts.getContext(0L)
          ScalaFuture(
            consumerPrimaryIndex
              .putNoOverwrite(transactionDB, new ConsumerTransaction(txn.name, txn.stream, txn.partition, txn.transactionID))
          ).as[TwitterFuture[Boolean]]
        case _ =>
          implicit val context = transactionService.Context.transactionContexts.getContext(0L)
          ScalaFuture(false).as[TwitterFuture[Boolean]]
      }
      result flatMap {value=>
        transactionDB.commit()
        TwitterFuture.value(value)
      }
    } else TwitterFuture.exception(tokenInvalidException)
  }


  override def putTransactions(token: String, transactions: Seq[Transaction]): TwitterFuture[Boolean] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      val transactionDB = environment.beginTransaction(null, null)
      val result = transactions map { transaction =>
        (transaction.producerTransaction, transaction.consumerTransaction) match {
          case (Some(txn), _) => getStreamTTL(txn.stream) flatMap {ttl =>
            implicit val context = transactionService.Context.transactionContexts.getContext(txn.partition, txn.stream.hashCode)
              ScalaFuture {
                val writeOptions = if (txn.state == Checkpointed) new WriteOptions().setTTL(checkTTL(ttl)) else new WriteOptions()
                val isNotExist =
                producerPrimaryIndex
                  .put(transactionDB, new ProducerTransaction(txn.transactionID, txn.state, txn.stream, txn.timestamp, txn.quantity, txn.partition), putType, writeOptions) != null
                if (isNotExist) {
                  logger.log(Level.INFO, s"${txn.toString} inserted/updated")
                  isNotExist
                } else {
                  logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
                  isNotExist
                }
              }.as[TwitterFuture[Boolean]]
            }
          case (_, Some(txn)) =>
            import transactionService.server.сonsumerService._
            implicit val context = transactionService.Context.transactionContexts.getContext(0L)
            ScalaFuture {
              val isNotExist =
                consumerPrimaryIndex
                  .put(transactionDB, new ConsumerTransaction(txn.name, txn.stream, txn.partition, txn.transactionID)) != null
              if (isNotExist) {
                logger.log(Level.INFO, s"${txn.toString} inserted/updated")
                isNotExist
              } else {
                logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
                isNotExist
              }
            }.as[TwitterFuture[Boolean]]
          case _ =>
            implicit val context = transactionService.Context.transactionContexts.getContext(0L)
            ScalaFuture(false).as[TwitterFuture[Boolean]]
        }
      }

      TwitterFuture.collect(result).flatMap { transactions =>
        val isOkay = transactions.forall(_ == true)
        if (isOkay) transactionDB.commit() else transactionDB.abort()
        TwitterFuture.value(isOkay)
      }
    } else TwitterFuture.exception(tokenInvalidException)
  }


  def scanTransactions(token: String, stream: String, partition: Int): TwitterFuture[Seq[Transaction]] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      TwitterFuture {
        import scala.collection.JavaConverters._

        val producerTransactions = producerPrimaryIndex
          .entities(
            new ProducerTransactionKey(stream, partition, Long.MinValue), false,
            new ProducerTransactionKey(stream, partition, Long.MaxValue), false
          ).iterator().asScala.toArray

        producerTransactions.map(txn => new Transaction {
          override def producerTransaction: Option[ProducerTransaction] = Some(txn)
          override def consumerTransaction: Option[ConsumerTransaction] = None
        })
      }
    } else TwitterFuture.exception(tokenInvalidException)
  }
}

object TransactionMetaServiceImpl {
  val storeName = resource.DB.TransactionMetaStoreName

  val directory = transactionService.io.FileUtils.createDirectory(resource.DB.TransactionMetaDirName)
  val environmentConfig = new EnvironmentConfig()
    .setAllowCreate(true)
    .setTransactional(true)
    .setTxnTimeout(resource.DB.TransactionMetaMaxTimeout, resource.DB.TransactionMetaTimeUnit)
  val storeConfig = new StoreConfig()
    .setAllowCreate(true)
    .setTransactional(true)
  val environment = new Environment(directory, environmentConfig)
  val entityStore = new EntityStore(environment, TransactionMetaServiceImpl.storeName, storeConfig)

  val producerPrimaryIndex = entityStore.getPrimaryIndex(classOf[ProducerTransactionKey], classOf[ProducerTransaction])
  val producerSecondaryIndex = entityStore.getSecondaryIndex(producerPrimaryIndex, classOf[Int], resource.DB.TransactionMetaProducerSecondaryIndexName)

  def close(): Unit = {
    entityStore.close()
    environment.close()
  }
}
