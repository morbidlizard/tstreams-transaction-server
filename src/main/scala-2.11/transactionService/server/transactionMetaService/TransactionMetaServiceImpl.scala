package transactionService.server.transactionMetaService


import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit._

import com.google.common.primitives.UnsignedBytes
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.sleepycat.je.{Transaction => _, _}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.rpc.TransactionStates.Checkpointed
import transactionService.rpc._
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
import transactionService.server.{Authenticable, CheckpointTTL}

import scala.collection.mutable.ArrayBuffer


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
    val streamObj = getStreamDatabaseObject(txn.stream)
    val writeOptions = if (txn.state == Checkpointed) new WriteOptions().setTTL(checkTTL(streamObj.ttl), HOURS) else new WriteOptions()
    val isNotExist = ProducerTransactionKey(txn, streamObj.streamNameToLong).put(database, null, putType, writeOptions) != null

     //logAboutTransactionExistence(isNotExist, txn.toString)

    isNotExist
  }


  private def putConsumerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ConsumerTransaction) = {
    import transactionService.server.сonsumerService._
    val streamNameToLong = getStreamDatabaseObject(txn.stream).streamNameToLong
    val isNotExist = ConsumerTransactionKey.apply(txn,streamNameToLong).put(database,databaseTxn, putType, new WriteOptions()) != null

    //logAboutTransactionExistence(isNotExist, txn.toString)
    isNotExist
  }

  private def putNoTransaction = false

  private def matchTransactionToPut(transaction: Transaction, transactionDB: com.sleepycat.je.Transaction) =
    (transaction.producerTransaction, transaction.consumerTransaction) match {
      case (Some(txn), _) => putProducerTransaction(transactionDB, txn)
      case (_, Some(txn)) => putConsumerTransaction(transactionDB, txn)
      case _ => putNoTransaction
    }


  override def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = authenticate(token) {
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


  private def doesProducerTransactionExpired(txn: transactionService.rpc.ProducerTransaction): Boolean =
    txn.timestamp <= Instant.now().getEpochSecond

  private def doesProducerTransactionExpired(txn: transactionService.server.transactionMetaService.ProducerTransaction): Boolean =
    txn.timestamp <= Instant.now().getEpochSecond

  private val comparator = UnsignedBytes.lexicographicalComparator
  override def scanTransactions(token: String, stream: String, partition: Int, from: Long, to: Long): TwitterFuture[Seq[Transaction]] =
    authenticate(token) {
      val lockMode = LockMode.DEFAULT
      val streamObj = getStreamDatabaseObject(stream)
      val transactionDB = environment.beginTransaction(null, null)
      val cursor = database.openCursor(transactionDB, CursorConfig.DEFAULT)

      def producerTransactionToTransaction(txn: ProducerTransactionKey) = {
        val producerTxn = transactionService.rpc.ProducerTransaction(streamObj.name, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.timestamp)
        Transaction(Some(producerTxn), None)
      }

      def moveCursorToKey: Option[ProducerTransactionKey] = {
        val keyFrom = new Key(streamObj.streamNameToLong, partition, from)
        val keyFound = keyFrom.toDatabaseEntry
        val dataFound = new DatabaseEntry()
        if (cursor.getSearchKey(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS)
          Some(new ProducerTransactionKey(keyFrom, ProducerTransaction.entryToObject(dataFound))) else None
      }

      moveCursorToKey match {
        case None =>
          cursor.close()
          transactionDB.commit()
          Array[Transaction]()

        case Some(producerTransactionKey) =>
          val txns = ArrayBuffer[ProducerTransactionKey](producerTransactionKey)
          val keyTo = new Key(streamObj.streamNameToLong, partition, to).toDatabaseEntry.getData
          val keyFound  = new DatabaseEntry()
          val dataFound = new DatabaseEntry()
          while (
            cursor.getNext(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS &&
              (comparator.compare(keyFound.getData, keyTo) <= 0)
          )
          {txns += ProducerTransactionKey(Key.entryToObject(keyFound), ProducerTransaction.entryToObject(dataFound))}

          cursor.close()
          transactionDB.commit()

          txns filterNot doesProducerTransactionExpired map producerTransactionToTransaction
      }
    }

//  private val transiteTxnsToInvalidState = new Runnable {
//    override def run(): Unit = {
//      import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
//      val transactionDB = environment.beginTransaction(null, null)
//      val cursor = secondaryDatabase.openCursor(transactionDB, CursorConfig.DEFAULT)
//
//      val keyFound  = new DatabaseEntry(java.nio.ByteBuffer.allocate(4).putInt(TransactionStates.Opened.value ^ 0x80000000).array())
//      val dataFound = new DatabaseEntry()
//      val pkKeyFound = new DatabaseEntry()
//
//      if (cursor.getSearchKey(keyFound, pkKeyFound ,dataFound, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
//        val txn = ProducerTransaction.entryToObject(dataFound)
//        if (doesProducerTransactionExpired(txn)) {
//          val newTxn = transactionService.server.transactionMetaService.ProducerTransaction(TransactionStates.Invalid, txn.quantity, txn.timestamp)
//            .toDatabaseEntry
//          database.put(transactionDB, pkKeyFound, newTxn)
//          logger.log(Level.INFO, s"${newTxn.toString} transit it's state to Invalid!")
//        }
//
//        while (cursor.getNextDup(keyFound, pkKeyFound, dataFound, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
//          val txn = ProducerTransaction.entryToObject(dataFound)
//          if (doesProducerTransactionExpired(txn)) {
//            val newTxn = transactionService.server.transactionMetaService.ProducerTransaction(TransactionStates.Invalid, txn.quantity, txn.timestamp)
//              .toDatabaseEntry
//            database.put(transactionDB, pkKeyFound, newTxn)
//            logger.log(Level.INFO, s"${newTxn.toString} transit it's state to Invalid!")
//         }
//        }
//      }
//
//      cursor.close()
//      transactionDB.commit()
//
//      import scala.collection.JavaConversions._
//      val entities = producerSecondaryIndexState.subIndex(TransactionStates.Opened.getValue()).entities(transactionDB, new CursorConfig().setReadUncommitted(true))
//      entities.iterator().sliding(configProperties.ServerConfig.transactionDataCleanAmount,configProperties.ServerConfig.transactionDataCleanAmount).foreach { txns =>
//        txns.filter(doesProducerTransactionExpired) foreach { txn =>
//          logger.log(Level.INFO, s"${txn.toString} transit it's state to Invalid!")
//          val newInvalidTxn = new ProducerTransaction(txn.transactionID, TransactionStates.Invalid, txn.stream.toLong, txn.timestamp, txn.quantity, txn.partition)
//          producerPrimaryIndex.put(transactionDB, newInvalidTxn)
//        }
//      }
//      entities.close()
//      transactionDB.commit()
//    }
//  }
//  Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("TransiteTxnsToInvalidState-%d").build()).scheduleWithFixedDelay(transiteTxnsToInvalidState,0, configProperties.ServerConfig.transactionTimeoutCleanOpened, java.util.concurrent.TimeUnit.SECONDS)
}

object TransactionMetaServiceImpl {
  import configProperties.DB

  val directory = transactionService.io.FileUtils.createDirectory(DB.TransactionMetaDirName)
  val environment = {
    val environmentConfig = new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setTxnTimeout(DB.TransactionMetaMaxTimeout, DB.TransactionMetaTimeUnit)
      .setLockTimeout(DB.TransactionMetaMaxTimeout, DB.TransactionMetaTimeUnit)

    val defaultDurability = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE)
    environmentConfig.setDurabilityVoid(defaultDurability)

    new Environment(directory, environmentConfig)
  }

  val database = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    val storeName = DB.TransactionMetaStoreName
    environment.openDatabase(null, storeName, dbConfig)
  }

//  val secondaryDatabase = {
//    val secondaryDatabaseName = "stateIndex"
//    val secondaryDatabaseConfig = new SecondaryConfig()
//    secondaryDatabaseConfig.setKeyCreator(ProducerTransactionStateIndex)
//    secondaryDatabaseConfig.setAllowCreate(true)
//    secondaryDatabaseConfig.setAllowPopulate(true)
//    secondaryDatabaseConfig.setTransactional(true)
//    secondaryDatabaseConfig.setSortedDuplicates(true)
//    environment.openSecondaryDatabase(null, secondaryDatabaseName, database, secondaryDatabaseConfig)
//  }

  def close(): Unit = {
    database.close()
    environment.close()
  }
}
