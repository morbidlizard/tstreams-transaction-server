package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit._

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, CheckpointTTL}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import com.google.common.primitives.UnsignedBytes
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.sleepycat.je.{Transaction => _, _}
import org.slf4j.{Logger, LoggerFactory}
import transactionService.rpc._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future => ScalaFuture}

trait TransactionMetadataServiceImpl extends TransactionMetaService[ScalaFuture]
  with Authenticable
  with CheckpointTTL {

  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  final val scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MarkTransactionsAsInvalid-%d").build())

  val directory = FileUtils.createDirectory(storageOpts.metadataDirectory, storageOpts.path)
  val transactionMetaEnviroment = {
    val environmentConfig = new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSharedCache(true)

//    config.berkeleyDBJEproperties foreach {
//      case (name, value) => environmentConfig.setConfigParam(name,value)
//    } //todo it will be deprecated soon

    val defaultDurability = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE)
    environmentConfig.setDurabilityVoid(defaultDurability)

    new Environment(directory, environmentConfig)
  }

  val producerTransactionsDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = storageOpts.metadataStorageName
    transactionMetaEnviroment.openDatabase(null, storeName, dbConfig)
  }

  val producerTransactionsWithOpenedStateDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = storageOpts.openedTransactionsStorageName
    transactionMetaEnviroment.openDatabase(null, storeName, dbConfig)
  }

  private final val putType = Put.OVERWRITE

  private def checkTTL(ttl: Long) = {
    val ttlInHours = MILLISECONDS.toHours(ttl.toLong).toInt
    if (ttlInHours == 0) 1 else ttlInHours
  }

  private def putProducerTransaction(databaseTransaction: com.sleepycat.je.Transaction, transaction: transactionService.rpc.ProducerTransaction): Boolean = {
    import transactionService.rpc.TransactionStates._
    val streamObj = getStreamDatabaseObject(transaction.stream)
    val producerTransaction = ProducerTransactionKey(transaction, streamObj.streamNameToLong)

    def logTransactionSaved(isSuccessfully: Boolean) =
      if (isSuccessfully) logger.debug(s"${producerTransaction.toString} is saved/updated")
      else logger.debug(s"${producerTransaction.toString} isn't saved/updated")

    val isOkay = transaction.state match {
        case Opened =>
          logger.debug(producerTransaction.toString)
          (producerTransaction.put(producerTransactionsWithOpenedStateDatabase, databaseTransaction, putType) != null) &&
            (producerTransaction.put(producerTransactionsDatabase, databaseTransaction, putType) != null)

        case Updated =>
          logger.debug(producerTransaction.toString)
          producerTransaction.put(producerTransactionsWithOpenedStateDatabase, databaseTransaction, putType) != null

        case Invalid =>
          (producerTransaction.delete(producerTransactionsWithOpenedStateDatabase, databaseTransaction) != null) &&
            (producerTransaction.delete(producerTransactionsDatabase, databaseTransaction) != null)

        case Checkpointed =>
          val writeOptions = new WriteOptions().setTTL(checkTTL(streamObj.ttl), HOURS)
          (producerTransaction.delete(producerTransactionsWithOpenedStateDatabase, databaseTransaction) != null) &&
            (producerTransaction.put(producerTransactionsDatabase, databaseTransaction, putType, writeOptions) != null)
        case _ => false
      }

    logTransactionSaved(isOkay)
    isOkay
  }


  def putConsumerTransaction(databaseTransaction: com.sleepycat.je.Transaction, transaction: ConsumerTransaction):Boolean //= {

//    val streamNameToLong = getStreamDatabaseObject(transaction.stream).streamNameToLong
//
//    ConsumerTransactionKey(transaction,streamNameToLong).put(producerTransactionsDatabase,databaseTransaction, putType, new WriteOptions()) != null

    //logAboutTransactionExistence(isNotExist, transaction.toString)
//  }

  private def putNoTransaction = false

  private def matchTransactionToPut(transaction: Transaction, transactionDB: com.sleepycat.je.Transaction): Boolean =
    (transaction.producerTransaction, transaction.consumerTransaction) match {
      case (Some(transaction), _) => scala.concurrent.blocking(putProducerTransaction(transactionDB, transaction))
      case (_, Some(transaction)) => scala.concurrent.blocking(putConsumerTransaction(transactionDB, transaction))
      case _ => putNoTransaction
    }

  override def putTransaction(token: Int, transaction: Transaction): ScalaFuture[Boolean] = authenticate(token) {
    val transactionDB = transactionMetaEnviroment.beginTransaction(null, new TransactionConfig())
    val isOkay =  matchTransactionToPut(transaction, transactionDB)
    if (isOkay) transactionDB.commit() else transactionDB.abort()
    isOkay
  }(executionContext.berkeleyWriteContext)



  override def putTransactions(token: Int, transactions: Seq[Transaction]): ScalaFuture[Boolean] = authenticate(token) {
    val transactionDB = transactionMetaEnviroment.beginTransaction(null, new TransactionConfig())

    @tailrec
    def processTransactions(transactions: List[Transaction]): Boolean = transactions match {
      case Nil => true
      case transaction::Nil => matchTransactionToPut(transaction, transactionDB)
      case transaction::transactions => if (matchTransactionToPut(transaction, transactionDB)) processTransactions(transactions) else false
    }

    val isOkay = processTransactions(transactions.toList)
    if (isOkay) transactionDB.commit() else transactionDB.abort()
    isOkay
  }(executionContext.berkeleyWriteContext)


  private def isProducerTransactionExpired(transaction: transactionService.rpc.ProducerTransaction): Boolean =
    (transaction.ttl + storageOpts.ttlAddMs) <= Instant.now().getEpochSecond

  private def isProducerTransactionExpired(transaction: ProducerTransaction): Boolean =
    (transaction.ttl + storageOpts.ttlAddMs) <= Instant.now().getEpochSecond

  private final val comparator = UnsignedBytes.lexicographicalComparator
  override def scanTransactions(token: Int, stream: String, partition: Int, from: Long, lastTransactionID: Long): ScalaFuture[Seq[Transaction]] =
    authenticate(token) {
      val lockMode = LockMode.READ_UNCOMMITTED_ALL
      val streamObj = getStreamDatabaseObject(stream)
      val transactionDB = transactionMetaEnviroment.beginTransaction(null, null)
      val cursor = producerTransactionsDatabase.openCursor(transactionDB, null)

      def producerTransactionToTransaction(transaction: ProducerTransactionKey) = {
        val producerTransaction = transactionService.rpc.ProducerTransaction(streamObj.name, transaction.partition, transaction.transactionID, transaction.state, transaction.quantity, transaction.ttl)
        Transaction(Some(producerTransaction), None)
      }

      def moveCursorToKey: Option[ProducerTransactionKey] = {
        val keyFrom = new Key(streamObj.streamNameToLong, partition, long2Long(from))
        val keyFound = keyFrom.toDatabaseEntry
        val dataFound = new DatabaseEntry()
        val transactionFrom = ProducerTransactionKey(Key.entryToObject(keyFrom.toDatabaseEntry),
          ProducerTransaction(state = TransactionStates.Opened, quantity = 1, ttl = 0L))

        if (cursor.getSearchKeyRange(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS) {
          var status = OperationStatus.SUCCESS
          val transactionFound = new ProducerTransactionKey(Key.entryToObject(keyFound), ProducerTransaction.entryToObject(dataFound))
          while (transactionFound.transactionID < transactionFrom.transactionID && status == OperationStatus.SUCCESS) {
            status = cursor.getNext(keyFound, dataFound, lockMode)
          }
          if(OperationStatus.SUCCESS == status)
            Some(transactionFound)
          else
            None
        } else
          None
      }

      moveCursorToKey match {
        case None =>
          cursor.close()
          transactionDB.commit()
          ArrayBuffer[Transaction]()

        case Some(producerTransactionKey) =>
          val transactions = ArrayBuffer[ProducerTransactionKey](producerTransactionKey)
          val keyFound  = new DatabaseEntry()
          val dataFound = new DatabaseEntry()
          var status = OperationStatus.SUCCESS
          var currentTransaction = producerTransactionKey
          while (currentTransaction.transactionID <= lastTransactionID && status == OperationStatus.SUCCESS) {
            status = cursor.getNext(keyFound, dataFound, lockMode)
            if(status == OperationStatus.SUCCESS) {
              currentTransaction = ProducerTransactionKey(Key.entryToObject(keyFound), ProducerTransaction.entryToObject(dataFound))
              if (currentTransaction.transactionID <= lastTransactionID && isProducerTransactionExpired(currentTransaction)) transactions += currentTransaction
            }
          }

          cursor.close()
          transactionDB.commit()

          transactions map producerTransactionToTransaction
      }
    }(executionContext.berkeleyReadContext)

  private val markTransactionsAsInvalid = new Runnable {
    logger.debug(s"Cleaner of expired transactions is running.")
    val cleanAmountPerDatabaseTransaction = storageOpts.clearAmount
    val lockMode = LockMode.READ_UNCOMMITTED_ALL

    override def run(): Unit = {
      val transactionDB = transactionMetaEnviroment.beginTransaction(null, null)
      val cursorProducerTransactions = producerTransactionsDatabase.openCursor(transactionDB, null)
      val cursorProducerTransactionsOpened = producerTransactionsWithOpenedStateDatabase.openCursor(transactionDB, null)


      def deleteExpiredTransactions(cursor: Cursor): Boolean = {
        val keyFound = new DatabaseEntry()
        val dataFound = new DatabaseEntry()

        if (cursor.getNext(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS) {
          val producerTransaction = ProducerTransaction.entryToObject(dataFound)
          if (isProducerTransactionExpired(producerTransaction)) {
            logger.debug(s"Cleaning $producerTransaction as it's expired.")
            cursor.delete()
            true
            // == OperationStatus.SUCCESS
          } else true
        } else false
      }


      @tailrec
      def repeat(counter: Int, cursor: Cursor): Unit = {
        val isExpired = deleteExpiredTransactions(cursor)
        if (counter > 0 && isExpired) repeat(counter - 1, cursor)
        else cursor.close()
      }

      repeat(cleanAmountPerDatabaseTransaction, cursorProducerTransactions)
      repeat(cleanAmountPerDatabaseTransaction, cursorProducerTransactionsOpened)
      transactionDB.commit()
    }
  }

  def closeTransactionMetaDatabases(): Unit = {
    scala.util.Try(producerTransactionsDatabase.close())
    scala.util.Try(producerTransactionsWithOpenedStateDatabase.close())
  }

  def closeTransactionMetaEnvironment() = {
    scala.util.Try(transactionMetaEnviroment.close())
  }

  scheduledExecutor.scheduleWithFixedDelay(markTransactionsAsInvalid, 0, storageOpts.clearDelayMs, java.util.concurrent.TimeUnit.SECONDS)
}
