package transactionService.server.transactionMetaService

import java.io.Closeable

import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.Context
import transactionService.server.Authenticable
import transactionService.rpc._
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
import transactionService.server.сonsumerService.ConsumerServiceImpl.consumerPrimaryIndex
import transactionService.exception.Throwables._

import scala.concurrent.{Future => ScalaFuture}


trait TransactionMetaServiceImpl extends TransactionMetaService[TwitterFuture]
  with Authenticable
{
  def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      val (producerTransactionOpt, consumerTransactionOpt) = (transaction.producerTransaction, transaction.consumerTransaction)

      val result = (producerTransactionOpt, consumerTransactionOpt) match {
        case (Some(txn), _) =>
          implicit val context = Context.transactionContexts.getContext(txn.partition, txn.stream.toInt)
          ScalaFuture {
            val isNotExist =
              producerPrimaryIndex
                .putNoOverwrite(new ProducerTransaction(txn.transactionID, txn.state, txn.stream, txn.timestamp, txn.quantity, txn.partition, txn.tll))
            if (isNotExist) {
              TransactionMetaServiceImpl.logger.log(Level.INFO, s"${txn.toString} inserted to DB!")
              isNotExist
            } else {
              TransactionMetaServiceImpl.logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
              isNotExist
            }
          }.as[TwitterFuture[Boolean]]
        case (_, Some(txn)) =>
          import transactionService.server.сonsumerService._
          implicit val context = transactionService.Context.transactionContexts.getContext(0L)
          ScalaFuture(
            consumerPrimaryIndex
              .putNoOverwrite(new ConsumerTransaction(txn.name, txn.stream, txn.partition, txn.transactionID))
          ).as[TwitterFuture[Boolean]]
        case _ =>
          implicit val context = transactionService.Context.transactionContexts.getContext(0L)
          ScalaFuture(false).as[TwitterFuture[Boolean]]
      }
      result flatMap TwitterFuture.value
    } else TwitterFuture.exception(tokenInvalidException)
  }

  override def putTransactions(token: String, transactions: Seq[Transaction]): TwitterFuture[Boolean] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      val transactionDB = environment.beginTransaction(null, null)
      val result = transactions map { transaction =>
        (transaction.producerTransaction, transaction.consumerTransaction) match {
          case (Some(txn), _) =>
            implicit val context = transactionService.Context.transactionContexts.getContext(txn.partition, txn.stream.toInt)
            ScalaFuture {
              val isNotExist =
                producerPrimaryIndex
                  .putNoOverwrite(transactionDB, new ProducerTransaction(txn.transactionID, txn.state, txn.stream, txn.timestamp, txn.quantity, txn.partition, txn.tll))
              if (isNotExist) {
                TransactionMetaServiceImpl.logger.log(Level.INFO, s"${txn.toString} inserted to DB!")
                isNotExist
              } else {
                TransactionMetaServiceImpl.logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
                isNotExist
              }
            }.as[TwitterFuture[Boolean]]
          case (_, Some(txn)) =>
            import transactionService.server.сonsumerService._
            implicit val context = transactionService.Context.transactionContexts.getContext(0L)
            ScalaFuture {
              val isNotExist =
                consumerPrimaryIndex
                  .putNoOverwrite(transactionDB, new ConsumerTransaction(txn.name, txn.stream, txn.partition, txn.transactionID))
              if (isNotExist) {
                TransactionMetaServiceImpl.logger.log(Level.INFO, s"${txn.toString} inserted to DB!")
                isNotExist
              } else {
                TransactionMetaServiceImpl.logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
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

//  def delTransaction(token: String, stream: String, partition: Int, transaction: Long): Future[Boolean] = {
//    val directory = new File(StreamServiceImpl.pathToDatabases)
//
//    val environmentConfig = new EnvironmentConfig()
//    val storeConfig = new StoreConfig()
//
//    val environment = new Environment(directory, environmentConfig)
//    val entityStore = new EntityStore(environment, TransactionMetaServiceImpl.storeName, storeConfig)
//
//    val result = Future {
//      val producerPrimaryKey = entityStore.getPrimaryIndex(
//        classOf[TransactionMetaServiceImpl.ProducerTransactionKey],
//        classOf[TransactionMetaServiceImpl.ProducerTransaction]
//      )
//      val txnOpt = Option(producerPrimaryKey.get(new TransactionMetaServiceImpl.ProducerTransactionKey(stream, partition, transaction)))
//      txnOpt match {
//        case Some(txn) => {
//          val txnToSave = new TransactionMetaServiceImpl.ProducerTransaction(txn.transactionID, TransactionStates.Invalid, txn.stream, txn.timestamp, txn.quantity, txn.partition)
//          if (producerPrimaryKey.put(txnToSave) != null) {
//            TransactionMetaServiceImpl.logger.log(Level.INFO, s"${txnToSave.toString} changes ${txn.state} state to ${TransactionStates.Invalid} state!")
//            true
//          } else {
//            TransactionMetaServiceImpl.logger.log(Level.ERROR, s"${txnToSave.toString}. Unexpected error.")
//            false
//          }
//        }
//        case None =>
//          TransactionMetaServiceImpl.logger.log(Level.WARNING, s"Producer transaction ${transaction.toString} doesn't exist!")
//          false
//      }
//    }
//
//    result flatMap { isMarked =>
//      entityStore.close()
//      environment.close()
//      Future.value(isMarked)
//    }
//  }

  def scanTransactions(token: String, stream: String, partition: Int): TwitterFuture[Seq[Transaction]] = ???

  def scanTransactionsCRC32(token: String, stream: String, partition: Int): TwitterFuture[Int] = ???
}

object TransactionMetaServiceImpl {
  val storeName = resource.DB.TransactionMetaStoreName
  val logger = Logger.get()

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

  def close(): Unit = {
    entityStore.close()
    environment.close()
  }
}
