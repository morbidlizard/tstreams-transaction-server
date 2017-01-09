package netty.server.transactionMetaService

import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit._

import com.google.common.primitives.UnsignedBytes
import com.sleepycat.je.{Transaction => _, _}

import scala.concurrent.{Promise, Future => ScalaFuture}
import transactionService.rpc._
import netty.server.transactionMetaService.TransactionMetaServiceImpl._
import netty.server.{Authenticable, CheckpointTTL}

import scala.collection.mutable.ArrayBuffer


trait TransactionMetaServiceImpl extends TransactionMetaService[ScalaFuture]
  with Authenticable
  with CheckpointTTL {

  private implicit val context = netty.Context.serverPool.getContext


//  val logger = Logger.get(this.getClass)

  private final val putType = Put.NO_OVERWRITE

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
  private def putProducerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ProducerTransaction): ScalaFuture[Boolean] = ScalaFuture {
    import transactionService.rpc.TransactionStates._
    val streamObj = getStreamDatabaseObject(txn.stream)
    val producerTransaction = ProducerTransactionKey(txn, streamObj.streamNameToLong)

  txn.state match {
        case Opened =>
          (producerTransaction.put(producerTransactionsWithOpenedStateDatabase, databaseTxn, putType) != null) &&
            (producerTransaction.put(producerTransactionsDatabase, databaseTxn, putType) != null)
        case Updated =>
          producerTransaction.put(producerTransactionsWithOpenedStateDatabase, databaseTxn, putType) != null

        case Invalid =>
          (producerTransaction.delete(producerTransactionsWithOpenedStateDatabase, databaseTxn) != null) &&
            (producerTransaction.delete(producerTransactionsDatabase, databaseTxn) != null)

        case Checkpointed =>
          val writeOptions = new WriteOptions().setTTL(checkTTL(streamObj.ttl), HOURS)
          (producerTransaction.delete(producerTransactionsWithOpenedStateDatabase, databaseTxn) != null) &&
            (producerTransaction.put(producerTransactionsDatabase, databaseTxn, putType, writeOptions) != null)
        case _ => false
      }
  }(netty.Context.berkeleyWritePool.getContext)


  private def putConsumerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ConsumerTransaction) = {
    import transactionService.server.сonsumerService._
    val streamNameToLong = getStreamDatabaseObject(txn.stream).streamNameToLong

    val isNotExist = Promise[Boolean]
    isNotExist success (
      ConsumerTransactionKey(txn,streamNameToLong).put(producerTransactionsDatabase,databaseTxn, putType, new WriteOptions()) != null
    )

    isNotExist.future
    //logAboutTransactionExistence(isNotExist, txn.toString)
  }

  private def putNoTransaction = ScalaFuture.successful(false)

  private def matchTransactionToPut(transaction: Transaction, transactionDB: com.sleepycat.je.Transaction): ScalaFuture[Boolean] =
    (transaction.producerTransaction, transaction.consumerTransaction) match {
      case (Some(txn), _) => putProducerTransaction(transactionDB, txn)
      case (_, Some(txn)) => putConsumerTransaction(transactionDB, txn)
      case _ => putNoTransaction
    }


  override def putTransaction(token: Int, transaction: Transaction): ScalaFuture[Boolean] = authenticateFutureBody(token) {
    val transactionDB = environment.beginTransaction(null, new TransactionConfig().setReadUncommitted(true))
    val result =  matchTransactionToPut(transaction, transactionDB)
    result.map {isOkay =>
      if (isOkay) transactionDB.commit() else transactionDB.abort()
      isOkay
    }
  }


  override def putTransactions(token: Int, transactions: Seq[Transaction]): ScalaFuture[Boolean] = authenticateFutureBody(token) {
    val transactionDB = environment.beginTransaction(null, new TransactionConfig().setReadUncommitted(true))
    val result = ScalaFuture.sequence(transactions map { transaction =>
      matchTransactionToPut(transaction, transactionDB)}
    )
    result map {operationStatuses =>
      val isOkay = operationStatuses.forall(_ == true)
      if (isOkay) transactionDB.commit() else transactionDB.abort()
      isOkay
    }
  }


  private def doesProducerTransactionExpired(txn: transactionService.rpc.ProducerTransaction): Boolean =
    (txn.keepAliveTTL + configProperties.ServerConfig.transactionMetadataTtlAdd) <= Instant.now().getEpochSecond

  private def doesProducerTransactionExpired(txn: netty.server.transactionMetaService.ProducerTransaction): Boolean =
    (txn.keepAliveTTL + configProperties.ServerConfig.transactionMetadataTtlAdd) <= Instant.now().getEpochSecond

  private val comparator = UnsignedBytes.lexicographicalComparator
  override def scanTransactions(token: Int, stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[Transaction]] =
    authenticate(token) {
      implicit val context = netty.Context.berkeleyReadPool.getContext
      val lockMode = LockMode.READ_UNCOMMITTED
      val streamObj = getStreamDatabaseObject(stream)
      val transactionDB = environment.beginTransaction(null, null)
      val cursor = producerTransactionsDatabase.openCursor(transactionDB, new CursorConfig().setReadUncommitted(true))

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
          ArrayBuffer[Transaction]()

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
    }(netty.Context.berkeleyReadPool.getContext)

//  private val transiteTxnsToInvalidState = new Runnable {
//    val cleanAmountPerDatabaseTransaction = configProperties.ServerConfig.transactionDataCleanAmount
//    val lockMode = LockMode.READ_UNCOMMITTED_ALL
//    override def run(): Unit = {
//      val transactionDB = environment.beginTransaction(null, new TransactionConfig().setReadUncommitted(true))
//      val cursorProducerTransactions = producerTransactionsDatabase.openCursor(transactionDB, new CursorConfig().setReadUncommitted(true))
//      val cursorProducerTransactionsOpened = producerTransactionsWithOpenedStateDatabase.openCursor(transactionDB, new CursorConfig().setReadUncommitted(true))
//
//      def deleteExpiredTransactions(cursor: Cursor): ScalaFuture[Boolean] = {
//        val keyFound = new DatabaseEntry()
//        val dataFound = new DatabaseEntry()
//
//        if (cursor.getNext(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS) {
//          val producerTransaction = ProducerTransaction.entryToObject(dataFound)
//          if (doesProducerTransactionExpired(producerTransaction)) {
//            ScalaFuture(cursor.delete() == OperationStatus.SUCCESS)(producerTransactionsContext)
//          } else ScalaFuture.successful(true)
//        } else ScalaFuture.successful(false)
//      }
//
//
//      def repeat(counter: Int, cursor: Cursor): ScalaFuture[Unit] = {
//        deleteExpiredTransactions(cursor) map (isExpired =>
//          if (counter > 0 && isExpired) repeat(counter - 1, cursor)
//          )
//      }
//
//      ScalaFuture.sequence(
//        Seq(
//          repeat(cleanAmountPerDatabaseTransaction, cursorProducerTransactions)
//            .map(_ =>  cursorProducerTransactions.close()),
//          repeat(cleanAmountPerDatabaseTransaction, cursorProducerTransactionsOpened)
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
      .setLockTimeout(5L, TimeUnit.SECONDS)

    configProperties.ServerConfig.berkeleyDBJEproperties foreach {
      case (name, value) => environmentConfig.setConfigParam(name,value)
    }

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
