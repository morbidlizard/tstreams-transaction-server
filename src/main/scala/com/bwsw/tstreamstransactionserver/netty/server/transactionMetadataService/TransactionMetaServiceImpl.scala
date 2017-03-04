package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.Authenticable
import com.bwsw.tstreamstransactionserver.netty.server.сonsumerService.ConsumerTransactionKey
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import com.google.common.primitives.UnsignedBytes
import com.sleepycat.je.{Transaction => _, _}
import org.slf4j.{Logger, LoggerFactory}
import transactionService.rpc._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future => ScalaFuture}

trait TransactionMetaServiceImpl extends TransactionStateHandler
  with Authenticable
{
  def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionKey], parentBerkeleyTxn :com.sleepycat.je.Transaction): Unit

  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val directory = FileUtils.createDirectory(storageOpts.metadataDirectory, storageOpts.path)
  private val defaultDurability = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE)
  val transactionMetaEnvironment = {
    val environmentConfig = new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSharedCache(true)

//    config.berkeleyDBJEproperties foreach {
//      case (name, value) => environmentConfig.setConfigParam(name,value)
//    } //todo it will be deprecated soon


    environmentConfig.setDurabilityVoid(defaultDurability)

    new Environment(directory, environmentConfig)
  }

  val producerTransactionsDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = storageOpts.metadataStorageName
    transactionMetaEnvironment.openDatabase(null, storeName, dbConfig)
  }


  val producerTransactionsWithOpenedStateDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = storageOpts.openedTransactionsStorageName
    transactionMetaEnvironment.openDatabase(null, storeName, dbConfig)
  }

  private def fillOpenedTransactionsRAMTable: java.util.concurrent.ConcurrentHashMap[Key, ProducerTransactionWithoutKey] = {
    val map = new java.util.concurrent.ConcurrentHashMap[Key, ProducerTransactionWithoutKey]()

    val keyFound  = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val cursor = producerTransactionsWithOpenedStateDatabase.openCursor(new DiskOrderedCursorConfig())
    while (cursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      map.put(Key.entryToObject(keyFound), ProducerTransactionWithoutKey.entryToObject(dataFound))
    }
    cursor.close()

    map
  }
  private val openedTransactionsMap: ConcurrentHashMap[Key, ProducerTransactionWithoutKey] = fillOpenedTransactionsRAMTable


  private final def calculateTTLForBerkeleyRecord(ttl: Long) = {
    val convertedTTL = TimeUnit.MILLISECONDS.toHours(ttl)
    if (convertedTTL == 0L) 1 else scala.math.abs(convertedTTL.toInt)
  }

  private def putTransactions(transactions: Seq[(Transaction, Long)], parentBerkeleyTxn: com.sleepycat.je.Transaction): Unit = {
    val (producerTransactions, consumerTransactions) = decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions)

    val nestedBerkeleyTxn = parentBerkeleyTxn

    val groupedProducerTransactionsWithTimestamp = groupProducerTransactionsByStream(producerTransactions)
    groupedProducerTransactionsWithTimestamp.foreach { case (stream, producerTransactionsWithTimestamp) =>
      val dbStream = getStreamDatabaseObject(stream)

      val dbProducerTransactions = decomposeProducerTransactionsToDatabaseRepresentation(dbStream, producerTransactionsWithTimestamp)
      val groupedProducerTransactions = groupProducerTransactions(dbProducerTransactions)
      groupedProducerTransactions foreach { case (key, txns) =>
        scala.util.Try {
          openedTransactionsMap.get(key) match {
            case data: ProducerTransactionWithoutKey =>
              val persistedProducerTransactionBerkeley = ProducerTransactionKey(key, data)
              if (!(persistedProducerTransactionBerkeley.state == TransactionStates.Checkpointed || persistedProducerTransactionBerkeley.state == TransactionStates.Invalid)) {
                val ProducerTransactionWithNewState = transiteProducerTransactionToNewState(persistedProducerTransactionBerkeley, txns)

                val binaryTxn = ProducerTransactionWithNewState.producerTransaction.toDatabaseEntry
                val binaryKey = key.toDatabaseEntry

                if (ProducerTransactionWithNewState.state == TransactionStates.Opened) {
                  scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.putNoOverwrite(nestedBerkeleyTxn, binaryKey, binaryTxn))
                }

                scala.concurrent.blocking(producerTransactionsDatabase.put(nestedBerkeleyTxn, binaryKey, binaryTxn, Put.OVERWRITE, new WriteOptions().setTTL(calculateTTLForBerkeleyRecord(dbStream.ttl))))
              }
            case _ =>
              val ProducerTransactionWithNewState = transiteProducerTransactionToNewState(txns)


              val binaryTxn = ProducerTransactionWithNewState.producerTransaction.toDatabaseEntry
              val binaryKey = key.toDatabaseEntry

              if (ProducerTransactionWithNewState.state == TransactionStates.Opened) {
                scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.putNoOverwrite(nestedBerkeleyTxn, binaryKey, binaryTxn))
              }

              scala.concurrent.blocking(producerTransactionsDatabase.put(nestedBerkeleyTxn, binaryKey, binaryTxn, Put.OVERWRITE, new WriteOptions().setTTL(calculateTTLForBerkeleyRecord(dbStream.ttl))))
          }
        }
//        match {
//          case scala.util.Success(_) => println("adasd")
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
    transactionMetaEnvironment.openDatabase(null, storeName, dbConfig)
  }


  class BigCommit(timestamp: Long, pathToFile: String) {
    private val transactionDB = transactionMetaEnvironment.beginTransaction(null, null)
    def putSomeTransactions(transactions: Seq[(Transaction, Long)]) = putTransactions(transactions, transactionDB)

    def commit(): Boolean = {
      val timestampCommitLog = TimestampCommitLog(timestamp, pathToFile)
      commitLogDatabase.putNoOverwrite(transactionDB, timestampCommitLog.keyToDatabaseEntry, timestampCommitLog.dataToDatabaseEntry)
      scala.util.Try(transactionDB.commit(defaultDurability)) match {
        case scala.util.Success(_) => true
        case scala.util.Failure(_) => false
      }
    }

    def abort(): Boolean  = scala.util.Try(transactionDB.abort()) match {
      case scala.util.Success(_) => true
      case scala.util.Failure(_) => false
    }
  }

  def getBigCommit(timestamp: Long, pathToFile: String) = new BigCommit(timestamp, pathToFile)

  private final val comparator = UnsignedBytes.lexicographicalComparator
  def scanTransactions(token: Int, stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[Transaction]] =
    authenticate(token) {
      val lockMode = LockMode.READ_UNCOMMITTED_ALL
      val streamObj = getStreamDatabaseObject(stream)
      val transactionDB = transactionMetaEnvironment.beginTransaction(null, null)
      val cursor = producerTransactionsDatabase.openCursor(transactionDB, null)

      def producerTransactionToTransaction(txn: ProducerTransactionKey) = {
        val producerTxn = transactionService.rpc.ProducerTransaction(streamObj.name, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.ttl)
        Transaction(Some(producerTxn), None)
      }

      val lastTransactionID = new Key(streamObj.streamNameToLong, partition, long2Long(to)).toDatabaseEntry.getData
      def moveCursorToKey: Option[ProducerTransactionKey] = {
        val keyFrom = new Key(streamObj.streamNameToLong, partition, long2Long(from))
        val keyFound = keyFrom.toDatabaseEntry
        val dataFound = new DatabaseEntry()
        val toStartFrom = cursor.getSearchKeyRange(keyFound, dataFound, lockMode)
        if (toStartFrom == OperationStatus.SUCCESS && comparator.compare(keyFound.getData, lastTransactionID) <= 0)
          Some(new ProducerTransactionKey(Key.entryToObject(keyFound), ProducerTransactionWithoutKey.entryToObject(dataFound))) else None
      }

      moveCursorToKey match {
        case None =>
          cursor.close()
          transactionDB.commit()
          ArrayBuffer[Transaction]()

        case Some(producerTransactionKey) =>
          val txns = ArrayBuffer[ProducerTransactionKey](producerTransactionKey)
          val transactionID  = new DatabaseEntry()
          val dataFound = new DatabaseEntry()
          while (
            cursor.getNext(transactionID, dataFound, lockMode) == OperationStatus.SUCCESS &&
              (comparator.compare(transactionID.getData, lastTransactionID) <= 0)
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
    private val cleanAmountPerDatabaseTransaction = storageOpts.clearAmount
    private val lockMode = LockMode.READ_UNCOMMITTED_ALL

    private def doesProducerTransactionExpired(producerTransactionWithoutKey: ProducerTransactionWithoutKey): Boolean = {
      (producerTransactionWithoutKey.timestamp + producerTransactionWithoutKey.ttl) >= timestampToDeleteTransactions
    }

    override def run(): Unit = {
      if (logger.isDebugEnabled) logger.debug(s"Cleaner of expired transactions is running.")
      val transactionDB = transactionMetaEnvironment.beginTransaction(null, null)
      val cursorProducerTransactionsOpened = producerTransactionsWithOpenedStateDatabase.openCursor(transactionDB, null)

      def deleteTransactionIfExpired(cursor: Cursor): Boolean = {
        val keyFound = new DatabaseEntry()
        val dataFound = new DatabaseEntry()

        if (cursor.getNext(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS) {
          val producerTransaction = ProducerTransactionWithoutKey.entryToObject(dataFound)
          val toDelete: Boolean = doesProducerTransactionExpired(producerTransaction)
          if (toDelete) {
            if (logger.isDebugEnabled) logger.debug(s"Cleaning $producerTransaction as it's expired.")
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

//      repeat(cleanAmountPerDatabaseTransaction, cursorProducerTransactionsOpened)
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
    scala.util.Try(transactionMetaEnvironment.close())
  }
}
