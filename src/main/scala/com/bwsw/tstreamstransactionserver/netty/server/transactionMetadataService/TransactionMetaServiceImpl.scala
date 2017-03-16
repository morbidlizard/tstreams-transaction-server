package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, StreamCache}
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionKey
import com.bwsw.tstreamstransactionserver.netty.server.streamService.KeyStream
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{KeyStreamPartition, TransactionStateHandler}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.sleepycat.je._
import org.slf4j.{Logger, LoggerFactory}
import transactionService.rpc._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future => ScalaFuture}

trait TransactionMetaServiceImpl extends TransactionStateHandler with StreamCache
  with Authenticable
{
  def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionKey], parentBerkeleyTxn :com.sleepycat.je.Transaction): Unit

  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val environment: Environment

  val producerTransactionsDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = storageOpts.metadataStorageName
    environment.openDatabase(null, storeName, dbConfig)
  }

  val producerTransactionsWithOpenedStateDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = storageOpts.openedTransactionsStorageName
    environment.openDatabase(null, storeName, dbConfig)
  }

  private def fillOpenedTransactionsRAMTable: com.google.common.cache.Cache[Key, ProducerTransactionWithoutKey] = {
    val secondsToLive = 300
    val threadsToWriteNumber = 1
    val cache = com.google.common.cache.CacheBuilder.newBuilder()
      .concurrencyLevel(threadsToWriteNumber)
      .expireAfterAccess(secondsToLive, TimeUnit.SECONDS)
      .build[Key, ProducerTransactionWithoutKey]()

    val keyFound  = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val cursor = producerTransactionsWithOpenedStateDatabase.openCursor(new DiskOrderedCursorConfig())
    while (cursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      cache.put(Key.entryToObject(keyFound), ProducerTransactionWithoutKey.entryToObject(dataFound))
    }
    cursor.close()

    cache
  }

  private val openedTransactionsRamTable: com.google.common.cache.Cache[Key, ProducerTransactionWithoutKey] = fillOpenedTransactionsRAMTable
  protected def getOpenedTransaction(key: Key): Option[ProducerTransactionWithoutKey] = {
    val transaction = openedTransactionsRamTable.getIfPresent(key)
    if (transaction != null) Some(transaction)
    else {
      val keyFound  = key.toDatabaseEntry
      val dataFound = new DatabaseEntry()

      if (producerTransactionsWithOpenedStateDatabase.get(null, keyFound, dataFound, null) == OperationStatus.SUCCESS) {
        val transactionOpt = ProducerTransactionWithoutKey.entryToObject(dataFound)
        openedTransactionsRamTable.put(key, transactionOpt)
        Some(transactionOpt)
      }
      else
        None
    }
  }

  private type Timestamp = Long
  private final def decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions: Seq[(transactionService.rpc.Transaction, Timestamp)]) = {
    val producerTransactions = ArrayBuffer[(ProducerTransaction, Timestamp)]()
    val consumerTransactions = ArrayBuffer[(ConsumerTransaction, Timestamp)]()

    transactions foreach {case(transaction, timestamp) =>
      (transaction.producerTransaction, transaction.consumerTransaction) match {
        case (Some(txn), _) =>
          val stream = getStreamFromOldestToNewest(txn.stream).last
          val key = KeyStreamPartition(stream.streamNameToLong, txn.partition)
          if (!isThatTransactionOutOfOrder(key, txn.transactionID)) {
            updateLastTransactionStreamPartitionRamTable(key, txn.transactionID)
            producerTransactions += ((txn, timestamp))
          }

        case (_, Some(txn)) =>
          val stream = getStreamFromOldestToNewest(txn.stream).last
          val key = KeyStreamPartition(stream.streamNameToLong, txn.partition)
          if (!isThatTransactionOutOfOrder(key, txn.transactionID))
            consumerTransactions += ((txn, timestamp))

        case  _ =>
      }
    }
    (producerTransactions, consumerTransactions)
  }

  private final def groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(txns: Seq[(ProducerTransaction, Timestamp)], berkeleyTransaction: com.sleepycat.je.Transaction): Map[KeyStream, ArrayBuffer[ProducerTransactionKey]] =
    txns.foldLeft[scala.collection.mutable.Map[KeyStream, ArrayBuffer[ProducerTransactionKey]]](scala.collection.mutable.Map()) { case (acc, (producerTransaction, timestamp)) =>
      val keyStreams = getStreamFromOldestToNewest(producerTransaction.stream)
      val streamForThisTransaction = keyStreams.filter(_.stream.timestamp <= timestamp).lastOption
      streamForThisTransaction match {
        case Some(keyStream) if !keyStream.stream.deleted =>
          val key = KeyStreamPartition(keyStream.streamNameToLong, producerTransaction.partition)
          putLastTransaction(key, producerTransaction.transactionID, berkeleyTransaction)

          if (acc.contains(keyStream))
            acc(keyStream) += ProducerTransactionKey(producerTransaction, keyStream.streamNameToLong, timestamp)
          else
            acc += ((keyStream, ArrayBuffer(ProducerTransactionKey(producerTransaction, keyStream.streamNameToLong, timestamp))))

          acc
        case _ => acc
      }
    }.toMap


  //  @throws[StreamNotExist]
  private final def decomposeConsumerTransactionsToDatabaseRepresentation(transactions: Seq[(ConsumerTransaction, Timestamp)]) = {
    val consumerTransactionsKey = ArrayBuffer[ConsumerTransactionKey]()
    transactions foreach { case (txn, timestamp) => scala.util.Try {
      val streamForThisTransaction = getStreamFromOldestToNewest(txn.stream).filter(_.stream.timestamp <= timestamp).last
      consumerTransactionsKey += ConsumerTransactionKey(txn, streamForThisTransaction.streamNameToLong, timestamp)
    }}

    consumerTransactionsKey
  }

  private final def groupProducerTransactions(producerTransactions: Seq[ProducerTransactionKey]) = producerTransactions.groupBy(txn => txn.key)


  private final def calculateTTLForBerkeleyRecord(ttl: Long) = {
    val convertedTTL = TimeUnit.SECONDS.toHours(ttl)
    if (convertedTTL == 0L) 1 else scala.math.abs(convertedTTL.toInt)
  }

  private def putTransactions(transactions: Seq[(transactionService.rpc.Transaction, Long)], parentBerkeleyTxn: com.sleepycat.je.Transaction): Unit = {
    val (producerTransactions, consumerTransactions) = decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions)
    val groupedProducerTransactionsWithTimestamp = groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(producerTransactions, parentBerkeleyTxn)

    groupedProducerTransactionsWithTimestamp.foreach { case (stream, dbProducerTransactions) =>
      val groupedProducerTransactions = groupProducerTransactions(dbProducerTransactions)
      groupedProducerTransactions foreach { case (key, txns) =>
        scala.util.Try {
          getOpenedTransaction(key) match {
            case Some(data: ProducerTransactionWithoutKey) =>
              val persistedProducerTransactionBerkeley = ProducerTransactionKey(key, data)
              if (!(persistedProducerTransactionBerkeley.state == TransactionStates.Checkpointed || persistedProducerTransactionBerkeley.state == TransactionStates.Invalid)) {
                val ProducerTransactionWithNewState = transitProducerTransactionToNewState(persistedProducerTransactionBerkeley, txns)

                val binaryTxn = ProducerTransactionWithNewState.producerTransaction.toDatabaseEntry
                val binaryKey = key.toDatabaseEntry

                openedTransactionsRamTable.put(ProducerTransactionWithNewState.key, ProducerTransactionWithNewState.producerTransaction)
                if (ProducerTransactionWithNewState.state == TransactionStates.Opened) {
                  scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.put(parentBerkeleyTxn, binaryKey, binaryTxn))
                } else {
                  scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.delete(parentBerkeleyTxn, binaryKey))
                }

                scala.concurrent.blocking(producerTransactionsDatabase.put(parentBerkeleyTxn, binaryKey, binaryTxn, Put.OVERWRITE, new WriteOptions().setTTL(calculateTTLForBerkeleyRecord(stream.ttl))))
              }
            case None =>
              val ProducerTransactionWithNewState = transitProducerTransactionToNewState(txns)

              val binaryTxn = ProducerTransactionWithNewState.producerTransaction.toDatabaseEntry
              val binaryKey = key.toDatabaseEntry

              openedTransactionsRamTable.put(ProducerTransactionWithNewState.key, ProducerTransactionWithNewState.producerTransaction)
              if (ProducerTransactionWithNewState.state == TransactionStates.Opened) {
                scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.put(parentBerkeleyTxn, binaryKey, binaryTxn))
              } else {
                scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.delete(parentBerkeleyTxn, binaryKey))
              }

              scala.concurrent.blocking(producerTransactionsDatabase.put(parentBerkeleyTxn, binaryKey, binaryTxn, Put.OVERWRITE, new WriteOptions().setTTL(calculateTTLForBerkeleyRecord(stream.ttl))))
          }
        }
//        match {
//          case scala.util.Success(_) => println(dbProducerTransactions, "asdasdasdasdasdasdasd")
//          case scala.util.Failure(throwable) => throwable.printStackTrace()
//        }
      }
    }
    putConsumerTransactions(decomposeConsumerTransactionsToDatabaseRepresentation(consumerTransactions), parentBerkeleyTxn)

    //    val isOkay = scala.util.Try(nestedBerkeleyTxn.commit()) match {
    //      case scala.util.Success(_) => true
    //      case scala.util.Failure(_) => false
    //    }
  }


  val commitLogDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = "CommitLogStore"
    environment.openDatabase(null, storeName, dbConfig)
  }


  class BigCommit(pathToFile: String) {
    private val transactionDB: com.sleepycat.je.Transaction = environment.beginTransaction(null, null)
    def putSomeTransactions(transactions: Seq[(transactionService.rpc.Transaction, Long)]) = putTransactions(transactions, transactionDB)

    def commit(fileCreationTimestamp: Long): Boolean = {
      val timestampCommitLog = TimestampCommitLog(fileCreationTimestamp, pathToFile)
      commitLogDatabase.putNoOverwrite(transactionDB, timestampCommitLog.keyToDatabaseEntry, timestampCommitLog.dataToDatabaseEntry)
      scala.util.Try(transactionDB.commit()) match {
        case scala.util.Success(_) => true
        case scala.util.Failure(_) => false
      }
    }

    def abort(): Boolean  = scala.util.Try(transactionDB.abort()) match {
      case scala.util.Success(_) => true
      case scala.util.Failure(_) => false
    }
  }

  def getBigCommit(pathToFile: String) = new BigCommit(pathToFile)

  def scanTransactions(stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[transactionService.rpc.Transaction]] =
    ScalaFuture {
      val lockMode = LockMode.READ_UNCOMMITTED_ALL
      val keyStream = getStreamFromOldestToNewest(stream).last
      val transactionDB = environment.beginTransaction(null, null)
      val cursor = producerTransactionsDatabase.openCursor(transactionDB, null)

      def producerTransactionToTransaction(txn: ProducerTransactionKey) = {
        val producerTxn = transactionService.rpc.ProducerTransaction(keyStream.name, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.ttl)
        transactionService.rpc.Transaction(Some(producerTxn), None)
      }

      val lastTransactionID = new Key(keyStream.streamNameToLong, partition, long2Long(to)).toDatabaseEntry
      def moveCursorToKey: Option[ProducerTransactionKey] = {
        val keyFrom = new Key(keyStream.streamNameToLong, partition, long2Long(from))
        val keyFound = keyFrom.toDatabaseEntry
        val dataFound = new DatabaseEntry()
        val toStartFrom = cursor.getSearchKeyRange(keyFound, dataFound, lockMode)
        if (toStartFrom == OperationStatus.SUCCESS && producerTransactionsDatabase.compareKeys(keyFound, lastTransactionID) <= 0)
          Some(new ProducerTransactionKey(Key.entryToObject(keyFound), ProducerTransactionWithoutKey.entryToObject(dataFound))) else None
      }

      moveCursorToKey match {
        case None =>
          cursor.close()
          transactionDB.commit()
          ArrayBuffer[transactionService.rpc.Transaction]()

        case Some(producerTransactionKey) =>
          val txns = ArrayBuffer[ProducerTransactionKey](producerTransactionKey)
          val transactionID  = new DatabaseEntry()
          val dataFound = new DatabaseEntry()
          while (
            cursor.getNext(transactionID, dataFound, lockMode) == OperationStatus.SUCCESS &&
              (producerTransactionsDatabase.compareKeys(transactionID, lastTransactionID) <= 0)
          )
          {
            txns += ProducerTransactionKey(Key.entryToObject(transactionID), ProducerTransactionWithoutKey.entryToObject(dataFound))
          }

          cursor.close()
          transactionDB.commit()

          txns map producerTransactionToTransaction
      }
    }(executionContext.berkeleyReadContext)


  final class TransactionsToDeleteTask(timestampToDeleteTransactions: Long) extends Runnable {
//    private val cleanAmountPerDatabaseTransaction = storageOpts.clearAmount
    private val lockMode = LockMode.READ_UNCOMMITTED_ALL

    private def doesProducerTransactionExpired(producerTransactionWithoutKey: ProducerTransactionWithoutKey): Boolean = {
      scala.math.abs(producerTransactionWithoutKey.timestamp + TimeUnit.SECONDS.toMillis(producerTransactionWithoutKey.ttl)) <= timestampToDeleteTransactions
    }

    private def transitToInvalidState(producerTransactionWithoutKey: ProducerTransactionWithoutKey) = {
      ProducerTransactionWithoutKey(TransactionStates.Invalid, producerTransactionWithoutKey.quantity, 0L, timestampToDeleteTransactions)
    }

    override def run(): Unit = {
      if (logger.isDebugEnabled) logger.debug(s"Cleaner of expired transactions is running.")
      val transactionDB = environment.beginTransaction(null, null)
      val cursorProducerTransactionsOpened = producerTransactionsWithOpenedStateDatabase.openCursor(transactionDB, null)

      def deleteTransactionIfExpired(cursor: Cursor): Boolean = {
        val keyFound = new DatabaseEntry()
        val dataFound = new DatabaseEntry()

        if (cursor.getNext(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS) {
          val producerTransactionWithoutKey = ProducerTransactionWithoutKey.entryToObject(dataFound)
          val toDelete: Boolean = doesProducerTransactionExpired(producerTransactionWithoutKey)
          if (toDelete) {
            if (logger.isDebugEnabled) logger.debug(s"Cleaning $producerTransactionWithoutKey as it's expired.")
            val canceledTransactionDueExpiration = transitToInvalidState(producerTransactionWithoutKey)
            producerTransactionsDatabase.put(transactionDB, keyFound, canceledTransactionDueExpiration.toDatabaseEntry)
            openedTransactionsRamTable.invalidate(Key.entryToObject(keyFound))
            cursor.delete()
            true
          } else true
        } else false
      }

      @tailrec
      def repeat(cursor: Cursor): Unit = {
        val doesExistAnyTransactionToDelete = deleteTransactionIfExpired(cursor)
        if (doesExistAnyTransactionToDelete) repeat(cursor)
        else cursor.close()
      }

      repeat(cursorProducerTransactionsOpened)
      transactionDB.commit()
    }
  }

  final def createTransactionsToDeleteTask(timestampToDeleteTransactions: Long) = new TransactionsToDeleteTask(timestampToDeleteTransactions)

//  scheduledExecutor.scheduleWithFixedDelay(markTransactionsAsInvalid, 0, storageOpts.clearDelayMs, java.util.concurrent.TimeUnit.SECONDS)

  def closeTransactionMetaDatabases(): Unit = {
    scala.util.Try(producerTransactionsDatabase.close())
    scala.util.Try(producerTransactionsWithOpenedStateDatabase.close())
  }

  def closeTransactionMetaEnvironment(): Unit = {
    scala.util.Try(environment.close())
  }
}
