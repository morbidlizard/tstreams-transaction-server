package transactionService.server.transactionMetaService


import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.sleepycat.je.{Transaction => _, _}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.rpc.TransactionStates.Checkpointed
import transactionService.rpc._
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
import transactionService.server.{Authenticable, CheckpointTTL}
import transactionService.server.сonsumerService.ConsumerServiceImpl.consumerPrimaryIndex


trait TransactionMetaServiceImpl extends TransactionMetaService[TwitterFuture]
  with Authenticable
  with CheckpointTTL {

  val logger = Logger.get(this.getClass)

  private final val putType = Put.OVERWRITE

  private def checkTTL(ttl: Int) = {
    val ttlInHours = MILLISECONDS.toHours(ttl.toLong).toInt
    if (ttlInHours == 0) 1 else ttlInHours
  }

  private def logAboutTransactionExistence(isNotExist: Boolean, transaction: String) = {
    if (isNotExist) {
      logger.log(Level.INFO, s"$transaction inserted/updated!")
    } else {
      logger.log(Level.WARNING, s"$transaction exists in DB!")
    }
  }

  private def putProducerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ProducerTransaction) = {
    val streamObj = getStream(txn.stream)
    val writeOptions = if (txn.state == Checkpointed) new WriteOptions().setTTL(checkTTL(streamObj.ttl), HOURS) else new WriteOptions()

    val isNotExist = producerPrimaryIndex
      .put(databaseTxn, new ProducerTransaction(txn.transactionID, txn.state, streamObj.streamNameToLong, txn.timestamp, txn.quantity, txn.partition), putType, writeOptions) != null

     logAboutTransactionExistence(isNotExist, txn.toString)

    isNotExist
  }


  private def putConsumerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ConsumerTransaction) = {
    import transactionService.server.сonsumerService._
    val streamNameToLong = getStream(txn.stream).streamNameToLong
    val isNotExist =
      consumerPrimaryIndex
        .put(databaseTxn, new ConsumerTransaction(txn.name, streamNameToLong, txn.partition, txn.transactionID), putType, new WriteOptions()) != null

    logAboutTransactionExistence(isNotExist, txn.toString)

    isNotExist
  }

  private def putNoTransaction = false

  private def matchTransactionToPut(transaction: Transaction, transactionDB: com.sleepycat.je.Transaction) =
    (transaction.producerTransaction, transaction.consumerTransaction) match {
      case (Some(txn), _) => putProducerTransaction(transactionDB, txn)
      case (_, Some(txn)) => putConsumerTransaction(transactionDB, txn)
      case _ => putNoTransaction
    }


  def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = authenticate(token) {
    val transactionDB = environment.beginTransaction(null, null)
    val result = matchTransactionToPut(transaction, transactionDB)
    if (result) transactionDB.commit() else transactionDB.abort()
    result
  }


  override def putTransactions(token: String, transactions: Seq[Transaction]): TwitterFuture[Boolean] = authenticate(token) {
    val transactionDB = environment.beginTransaction(null, null)
    val result = transactions map { transaction =>
      matchTransactionToPut(transaction, transactionDB)
    }
    val isOkay = result.forall(_ == true)
    if (isOkay) transactionDB.commit() else transactionDB.abort()
    isOkay
  }

  def scanTransactions(token: String, stream: String, partition: Int): TwitterFuture[Seq[Transaction]] =
    authenticate(token) {
      import scala.collection.JavaConverters._
      val streamObj = getStream(stream)
      val transactionDB = environment.beginTransaction(null, null)
      val entitiesProducetTxns = producerPrimaryIndex.entities(
          transactionDB,
          new ProducerTransactionKey(streamObj.streamNameToLong, partition, Long.MinValue), false,
          new ProducerTransactionKey(streamObj.streamNameToLong, partition, Long.MaxValue), false,
          new CursorConfig().setReadCommitted(true)
      )

      val producerTransactions = entitiesProducetTxns.iterator().asScala.toArray.map(txn => ProducerTransaction(streamObj.name,txn.partition,txn.transactionID,txn.state,txn.quantity,txn.timestamp))
      entitiesProducetTxns.close()
      transactionDB.commitNoSync()
      producerTransactions.map(txn => Transaction(Some(txn), None))
    }

  private val transiteTxnsToInvalidState = new Runnable {
    override def run(): Unit = {
      import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
      val transactionDB = environment.beginTransaction(null, null)

      import scala.collection.JavaConversions._
      val entities = producerSecondaryIndexState.subIndex(TransactionStates.Opened.getValue()).entities(transactionDB, new CursorConfig().setReadUncommitted(true))
      entities.iterator().toArray foreach{txn =>
        logger.log(Level.INFO, s"${txn.toString} transit it's state to Invalid!")
        val newInvalidTxn = new ProducerTransaction(txn.transactionID, TransactionStates.Invalid, txn.stream.toLong, txn.timestamp, txn.quantity, txn.partition)
        producerPrimaryIndex.put(transactionDB, newInvalidTxn)
      }

      entities.close()
      transactionDB.commit()
    }
  }
  Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("TransiteTxnsToInvalidState-%d").build()).scheduleWithFixedDelay(transiteTxnsToInvalidState,0, configProperties.ServerConfig.transactionTimeoutCleanOpened, java.util.concurrent.TimeUnit.SECONDS)
}

object TransactionMetaServiceImpl {

  import configProperties.DB

  val storeName = DB.TransactionMetaStoreName

  val directory = transactionService.io.FileUtils.createDirectory(DB.TransactionMetaDirName)
  val environmentConfig = new EnvironmentConfig()
    .setAllowCreate(true)
    .setTransactional(true)
    .setTxnTimeout(DB.TransactionMetaMaxTimeout, DB.TransactionMetaTimeUnit)
    .setLockTimeout(DB.TransactionMetaMaxTimeout, DB.TransactionMetaTimeUnit)

  val defaultDurability = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE)
  environmentConfig.setDurabilityVoid(defaultDurability)


  val storeConfig = new StoreConfig()
    .setAllowCreate(true)
    .setTransactional(true)

  val environment = new Environment(directory, environmentConfig)

  val entityStore = new EntityStore(environment, TransactionMetaServiceImpl.storeName, storeConfig)

  val producerPrimaryIndex = entityStore.getPrimaryIndex(classOf[ProducerTransactionKey], classOf[ProducerTransaction])
  val producerSecondaryIndexState = entityStore.getSecondaryIndex(producerPrimaryIndex, classOf[Int], DB.TransactionMetaProducerSecondaryIndexState)

  def close(): Unit = {
    entityStore.close()
    environment.close()
  }
}
