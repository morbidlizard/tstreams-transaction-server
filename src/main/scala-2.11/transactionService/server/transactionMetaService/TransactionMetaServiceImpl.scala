package transactionService.server.transactionMetaService


import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit._

import com.google.common.primitives.UnsignedBytes
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.sleepycat.je.{Transaction => _, _}
import com.twitter.logging.Logger
import com.twitter.util.{Future => TwitterFuture}
import transactionService.rpc._
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
import transactionService.server.{Authenticable, CheckpointTTL}

import scala.annotation.tailrec
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

//  private def logAboutTransactionExistence(isNotExist: Boolean, transaction: String) = {
//    if (isNotExist) {
//      logger.log(Level.INFO, s"$transaction inserted/updated!")
//    } else {
//      logger.log(Level.WARNING, s"$transaction exists in DB!")
//    }
//  }

  private val producerTransactionsContext = transactionService.Context.producerTransactionsContext.getContext
  private def putProducerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ProducerTransaction) = {
    import transactionService.rpc.TransactionStates._
    val streamObj = getStreamDatabaseObject(txn.stream)
    val producerTransaction = ProducerTransactionKey(txn, streamObj.streamNameToLong)



    txn.state match {
      case Opened =>
        producerTransactionsContext(
          (producerTransaction.put(producerTransactionsWithOpenedStateDatabase, databaseTxn, putType) != null) &&
            (producerTransaction.put(producerTransactionsDatabase, databaseTxn, putType) != null)
        )

      case Updated =>
        producerTransactionsContext(producerTransaction.put(producerTransactionsWithOpenedStateDatabase, databaseTxn, putType) != null)
          .map(_ == true)

      case Invalid =>
        producerTransactionsContext(
          (producerTransaction.delete(producerTransactionsWithOpenedStateDatabase, databaseTxn) != null) &&
            (producerTransaction.delete(producerTransactionsDatabase, databaseTxn) != null)
        )

      case Checkpointed =>
        val writeOptions = new WriteOptions().setTTL(checkTTL(streamObj.ttl), HOURS)
        producerTransactionsContext(
          (producerTransaction.delete(producerTransactionsWithOpenedStateDatabase, databaseTxn) != null) &&
            (producerTransaction.put(producerTransactionsDatabase, databaseTxn, putType, writeOptions) != null)
        )

      case _ => TwitterFuture.value(false)
    }
  }


  private def putConsumerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ConsumerTransaction) = {
    import transactionService.server.сonsumerService._
    val streamNameToLong = getStreamDatabaseObject(txn.stream).streamNameToLong
    val isNotExist = producerTransactionsContext(ConsumerTransactionKey.apply(txn,streamNameToLong).put(producerTransactionsDatabase,databaseTxn, putType, new WriteOptions()) != null)

    //logAboutTransactionExistence(isNotExist, txn.toString)
    isNotExist
  }

  private def putNoTransaction = TwitterFuture.value(false)

  private def matchTransactionToPut(transaction: Transaction, transactionDB: com.sleepycat.je.Transaction) =
    (transaction.producerTransaction, transaction.consumerTransaction) match {
      case (Some(txn), _) => putProducerTransaction(transactionDB, txn)
      case (_, Some(txn)) => putConsumerTransaction(transactionDB, txn)
      case _ => putNoTransaction
    }


  override def putTransaction(token: Int, transaction: Transaction): TwitterFuture[Boolean] = authenticateFutureBody(token) {
    val transactionDB = environment.beginTransaction(null, new TransactionConfig().setReadUncommitted(true))
    val result = matchTransactionToPut(transaction, transactionDB)
    result map {isOkay =>
      if (isOkay) transactionDB.commit() else transactionDB.abort()
      isOkay
    }
  }


  override def putTransactions(token: Int, transactions: Seq[Transaction]): TwitterFuture[Boolean] = authenticateFutureBody(token) {
    val transactionDB = environment.beginTransaction(null, new TransactionConfig().setReadUncommitted(true))
    val result = TwitterFuture.collect(transactions map { transaction =>
      matchTransactionToPut(transaction, transactionDB)
    })
    result map {operationStatuses =>
      val isOkay = operationStatuses.forall(_ == true)
      if (isOkay) transactionDB.commit() else transactionDB.abort()
      isOkay
    }
  }


  private def doesProducerTransactionExpired(txn: transactionService.rpc.ProducerTransaction): Boolean =
    (txn.keepAliveTTL + configProperties.ServerConfig.transactionMetadataTtlAdd) <= Instant.now().getEpochSecond

  private def doesProducerTransactionExpired(txn: transactionService.server.transactionMetaService.ProducerTransaction): Boolean =
    (txn.keepAliveTTL + configProperties.ServerConfig.transactionMetadataTtlAdd) <= Instant.now().getEpochSecond

  private val comparator = UnsignedBytes.lexicographicalComparator
  override def scanTransactions(token: Int, stream: String, partition: Int, from: Long, to: Long): TwitterFuture[Seq[Transaction]] =
    authenticate(token) {
      val lockMode = LockMode.DEFAULT
      val streamObj = getStreamDatabaseObject(stream)
      val transactionDB = environment.beginTransaction(null, null)
      val cursor = producerTransactionsDatabase.openCursor(transactionDB, CursorConfig.DEFAULT)

      def producerTransactionToTransaction(txn: ProducerTransactionKey) = {
        val producerTxn = transactionService.rpc.ProducerTransaction(streamObj.name, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.keepAliveTTL)
        Transaction(Some(producerTxn), None)
      }

      def moveCursorToKey: Option[ProducerTransactionKey] = {
        val keyFrom = new Key(streamObj.streamNameToLong, partition, long2Long(from))
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
          val keyTo = new Key(streamObj.streamNameToLong, partition, long2Long(to)).toDatabaseEntry.getData
          val keyFound  = new DatabaseEntry()
          val dataFound = new DatabaseEntry()
          while (
            cursor.getNext(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS &&
              (comparator.compare(keyFound.getData, keyTo) <= 0)
          )
          {
            val txn = ProducerTransactionKey(Key.entryToObject(keyFound), ProducerTransaction.entryToObject(dataFound))
            if (doesProducerTransactionExpired(txn)) txns += txn
          }

          cursor.close()
          transactionDB.commit()

          txns map producerTransactionToTransaction
      }
    }

//  private val transiteTxnsToInvalidState = new Runnable {
//    val cleanAmountPerDatabaseTransaction = configProperties.ServerConfig.transactionDataCleanAmount
//    val lockMode = LockMode.READ_UNCOMMITTED
//    override def run(): Unit = {
//      val transactionDB = environment.beginTransaction(null, null)
//      val cursorProducerTransactions = producerTransactionsDatabase.openCursor(transactionDB, new CursorConfig().setReadUncommitted(true))
//      val cursorProducerTransactionsOpened = producerTransactionsWithOpenedStateDatabase.openCursor(transactionDB, new CursorConfig().setReadUncommitted(true))
//
//      def deleteExpiredTransactions(cursor: Cursor): Boolean = {
//        val keyFound = new DatabaseEntry()
//        val dataFound = new DatabaseEntry()
//        if (cursor.getNext(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS) {
//          val producerTransaction = ProducerTransaction.entryToObject(dataFound)
//          if (doesProducerTransactionExpired(producerTransaction)) {
//            cursor.delete() == OperationStatus.SUCCESS
//          } else false
//        } else false
//      }
//
//      @tailrec
//      def repeat(counter: Int, cursor: Cursor): Unit = {
//        if (counter > 0 && deleteExpiredTransactions(cursor)) repeat(counter - 1, cursor)
//      }
//
//      TwitterFuture.collect(
//        Seq(
//          TwitterFuture(repeat(cleanAmountPerDatabaseTransaction, cursorProducerTransactions))
//            .map(_ =>  cursorProducerTransactions.close()),
//          TwitterFuture(repeat(cleanAmountPerDatabaseTransaction, cursorProducerTransactionsOpened))
//            .map(_ =>  cursorProducerTransactionsOpened.close())
//        )
//      ).map(_ => transactionDB.commit())
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

  val producerTransactionsDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    val storeName = DB.TransactionMetaStoreName
    environment.openDatabase(null, storeName, dbConfig)
  }

  val producerTransactionsWithOpenedStateDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    val storeName = DB.TransactionMetaOpenStoreName
    environment.openDatabase(null, storeName, dbConfig)
  }


  def close(): Unit = {
    producerTransactionsDatabase.close()
    environment.close()
  }
}
