package com.bwsw.tstreamstransactionserver.netty.server.transactionMetaService

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

trait TransactionMetaServiceImpl extends TransactionMetaService[ScalaFuture]
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

  private def putProducerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: transactionService.rpc.ProducerTransaction): Boolean = {
    import transactionService.rpc.TransactionStates._
    val streamObj = getStreamDatabaseObject(txn.stream)
    val producerTransaction = ProducerTransactionKey(txn, streamObj.streamNameToLong)

    def logTransactionSaved(isSuccessfully: Boolean) =
      if (isSuccessfully) logger.debug(s"${producerTransaction.toString} is saved/updated")
      else logger.debug(s"${producerTransaction.toString} isn't saved/updated")

    val isOkay = txn.state match {
        case Opened =>
          logger.debug(producerTransaction.toString)
          (producerTransaction.put(producerTransactionsWithOpenedStateDatabase, databaseTxn, putType) != null) &&
            (producerTransaction.put(producerTransactionsDatabase, databaseTxn, putType) != null)

        case Updated =>
          logger.debug(producerTransaction.toString)
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

    logTransactionSaved(isOkay)
    isOkay
  }


  def putConsumerTransaction(databaseTxn: com.sleepycat.je.Transaction, txn: ConsumerTransaction):Boolean //= {

//    val streamNameToLong = getStreamDatabaseObject(txn.stream).streamNameToLong
//
//    ConsumerTransactionKey(txn,streamNameToLong).put(producerTransactionsDatabase,databaseTxn, putType, new WriteOptions()) != null

    //logAboutTransactionExistence(isNotExist, txn.toString)
//  }

  private def putNoTransaction = false

  private def matchTransactionToPut(transaction: Transaction, transactionDB: com.sleepycat.je.Transaction): Boolean =
    (transaction.producerTransaction, transaction.consumerTransaction) match {
      case (Some(txn), _) => scala.concurrent.blocking(putProducerTransaction(transactionDB, txn))
      case (_, Some(txn)) => scala.concurrent.blocking(putConsumerTransaction(transactionDB, txn))
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
      case txn::Nil => matchTransactionToPut(txn, transactionDB)
      case txn::txns => if (matchTransactionToPut(txn, transactionDB)) processTransactions(txns) else false
    }

    val isOkay = processTransactions(transactions.toList)
    if (isOkay) transactionDB.commit() else transactionDB.abort()
    isOkay
  }(executionContext.berkeleyWriteContext)


  private def doesProducerTransactionExpired(txn: transactionService.rpc.ProducerTransaction): Boolean =
    (txn.ttl + storageOpts.ttlAddMs) <= Instant.now().getEpochSecond

  private def doesProducerTransactionExpired(txn: ProducerTransaction): Boolean =
    (txn.ttl + storageOpts.ttlAddMs) <= Instant.now().getEpochSecond

  private final val comparator = UnsignedBytes.lexicographicalComparator
  override def scanTransactions(token: Int, stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[Transaction]] =
    authenticate(token) {
      val lockMode = LockMode.READ_UNCOMMITTED_ALL
      val streamObj = getStreamDatabaseObject(stream)
      val transactionDB = transactionMetaEnviroment.beginTransaction(null, null)
      val cursor = producerTransactionsDatabase.openCursor(transactionDB, null)

      def producerTransactionToTransaction(txn: ProducerTransactionKey) = {
        val producerTxn = transactionService.rpc.ProducerTransaction(streamObj.name, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.ttl)
        Transaction(Some(producerTxn), None)
      }

      def moveCursorToKey: Option[ProducerTransactionKey] = {
        val keyFrom = new Key(streamObj.streamNameToLong, partition, long2Long(from))
        val keyFound = keyFrom.toDatabaseEntry
        val dataFound = new DatabaseEntry()
        if (cursor.getSearchKeyRange(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS)
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
          if (doesProducerTransactionExpired(producerTransaction)) {
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
    Option(producerTransactionsDatabase.close())
    Option(producerTransactionsWithOpenedStateDatabase.close())
  }

  def closeTransactionMetaEnviroment() = {
    Option(transactionMetaEnviroment.close())
  }

  scheduledExecutor.scheduleWithFixedDelay(markTransactionsAsInvalid, 0, storageOpts.clearDelayMs, java.util.concurrent.TimeUnit.SECONDS)
}
