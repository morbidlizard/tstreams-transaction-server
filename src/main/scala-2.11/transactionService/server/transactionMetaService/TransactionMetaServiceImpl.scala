package transactionService.server.transactionMetaService


import com.sleepycat.je.{Environment, EnvironmentConfig, Put, WriteOptions}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.Context
import transactionService.server.{Authenticable, CheckpointTTL}
import transactionService.rpc._
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
import transactionService.server.сonsumerService.ConsumerServiceImpl.consumerPrimaryIndex
import transactionService.rpc.TransactionStates.Checkpointed


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


  private def putProducerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ProducerTransaction) = {
    getStreamTTL(txn.stream) flatMap {ttl =>
      val futurePool = Context.transactionContexts.getContext(txn.stream.hashCode, txn.partition)
      futurePool {
        val writeOptions = if (txn.state == Checkpointed) new WriteOptions().setTTL(checkTTL(ttl)) else new WriteOptions()
        val isNotExist =
          producerPrimaryIndex
            .put(databaseTxn, new ProducerTransaction(txn.transactionID, txn.state, txn.stream, txn.timestamp, txn.quantity, txn.partition), putType, writeOptions) != null
        if (isNotExist) {
          logger.log(Level.INFO, s"${txn.toString} inserted/updated!")
          isNotExist
        } else {
          logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
          isNotExist
        }
      }
    }
  }


  private def putConsumerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ConsumerTransaction) = {
    import transactionService.server.сonsumerService._
    val futurePool = transactionService.Context.transactionContexts.getContext(0L)
    futurePool {
      val isNotExist =
        consumerPrimaryIndex
          .put(databaseTxn, new ConsumerTransaction(txn.name, txn.stream, txn.partition, txn.transactionID), putType, new WriteOptions()) != null
      if (isNotExist) {
        logger.log(Level.INFO, s"${txn.toString} inserted/updated!")
        isNotExist
      } else {
        logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
        isNotExist
      }
    }
  }

  private def putNoTransaction = TwitterFuture.value(false)


  def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = authenticateFutureBody(token) {
      val transactionDB = environment.beginTransaction(null, null)
      val (producerTransactionOpt, consumerTransactionOpt) = (transaction.producerTransaction, transaction.consumerTransaction)

      val result = (producerTransactionOpt, consumerTransactionOpt) match {
        case (Some(txn), _) => putProducerTransaction(transactionDB, txn)
        case (_, Some(txn)) => putConsumerTransaction(transactionDB, txn)
        case _ => putNoTransaction
      }

      result flatMap {isOkay=>
        if (isOkay) transactionDB.commit() else transactionDB.abort()
        TwitterFuture.value(isOkay)
      }
  }


  override def putTransactions(token: String, transactions: Seq[Transaction]): TwitterFuture[Boolean] = authenticateFutureBody(token) {
    val transactionDB = environment.beginTransaction(null, null)
    val result = transactions map { transaction =>
      (transaction.producerTransaction, transaction.consumerTransaction) match {
        case (Some(txn), _) => putProducerTransaction(transactionDB, txn)
        case (_, Some(txn)) => putConsumerTransaction(transactionDB, txn)
        case _ => putNoTransaction
      }
    }

    TwitterFuture.collect(result).flatMap { transactions =>
      val isOkay = transactions.forall(_ == true)
      if (isOkay) transactionDB.commit() else transactionDB.abort()
      TwitterFuture.value(isOkay)
    }
  }


  def scanTransactions(token: String, stream: String, partition: Int): TwitterFuture[Seq[Transaction]] =
    authenticate(token) {
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
}

object TransactionMetaServiceImpl {
  import configProperties.DB
  val storeName = DB.TransactionMetaStoreName

  val directory = transactionService.io.FileUtils.createDirectory(DB.TransactionMetaDirName)
  val environmentConfig = new EnvironmentConfig()
    .setAllowCreate(true)
    .setTransactional(true)
    .setTxnTimeout(DB.TransactionMetaMaxTimeout, DB.TransactionMetaTimeUnit)
  val storeConfig = new StoreConfig()
    .setAllowCreate(true)
    .setTransactional(true)
  val environment = new Environment(directory, environmentConfig)
  val entityStore = new EntityStore(environment, TransactionMetaServiceImpl.storeName, storeConfig)

  val producerPrimaryIndex = entityStore.getPrimaryIndex(classOf[ProducerTransactionKey], classOf[ProducerTransaction])
  val producerSecondaryIndex = entityStore.getSecondaryIndex(producerPrimaryIndex, classOf[Int], DB.TransactionMetaProducerSecondaryIndexName)

  def close(): Unit = {
    entityStore.close()
    environment.close()
  }
}
