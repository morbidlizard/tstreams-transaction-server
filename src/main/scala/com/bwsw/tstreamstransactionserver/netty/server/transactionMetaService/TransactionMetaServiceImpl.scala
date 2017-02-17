package com.bwsw.tstreamstransactionserver.netty.server.transactionMetaService

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, CheckpointTTL}
import com.bwsw.tstreamstransactionserver.netty.server.сonsumerService.ConsumerTransactionKey
import com.bwsw.tstreamstransactionserver.options.StorageOptions
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import com.google.common.primitives.UnsignedBytes
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.sleepycat.je.{Transaction => _, _}
import org.slf4j.{Logger, LoggerFactory}
import transactionService.rpc._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future => ScalaFuture}

trait TransactionMetaServiceImpl extends TransactionStateHandler
  with Authenticable
{
  def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionKey], parentBerkeleyTxn :com.sleepycat.je.Transaction): Boolean

  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

//  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  final val scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MarkTransactionsAsInvalid-%d").build())

  val directory = FileUtils.createDirectory(storageOpts.metadataDirectory, storageOpts.path)
  private val defaultDurability = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE)
  val transactionMetaEnviroment = {
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
    transactionMetaEnviroment.openDatabase(null, storeName, dbConfig)
  }

  val producerTransactionsWithOpenedStateDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = storageOpts.openedTransactionsStorageName
    transactionMetaEnviroment.openDatabase(null, storeName, dbConfig)
  }

  private def fillOpenedTransactionsRAMTable: java.util.concurrent.ConcurrentHashMap[Key, ProducerTransactionWithoutKey] = {
    val map = new java.util.concurrent.ConcurrentHashMap[Key, ProducerTransactionWithoutKey]()

    val lockMode = LockMode.READ_UNCOMMITTED_ALL
    val keyFound  = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val cursor = producerTransactionsWithOpenedStateDatabase.openCursor(new DiskOrderedCursorConfig())
    while (cursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      map.put(Key.entryToObject(keyFound), ProducerTransactionWithoutKey.entryToObject(dataFound))
    }
    map
  }
  private val openedTransactionsMap: ConcurrentHashMap[Key, ProducerTransactionWithoutKey] = fillOpenedTransactionsRAMTable


  private final def calculateTTLForBerkeleyRecord(ttl: Int) = {
    val convertedTTL = TimeUnit.MILLISECONDS.toHours(ttl.toLong)
    if (convertedTTL == 0L) 1 else convertedTTL.toInt
  }

  private def putTransactions(transactions: Seq[(Transaction, Long)], parentBerkeleyTxn: com.sleepycat.je.Transaction): Boolean = {
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
              val dbTransaction = ProducerTransactionKey(key, data)
              if (!(dbTransaction.state == TransactionStates.Checkpointed || dbTransaction.state == TransactionStates.Invalid)) {
                val ProducerTransactionWithNewState = transiteProducerTransactionToNewState(dbTransaction, txns)

                val binaryTxn = ProducerTransactionWithNewState.producerTransaction.toDatabaseEntry
                val binaryKey = key.toDatabaseEntry

                if (ProducerTransactionWithNewState.state == TransactionStates.Opened) {
                  scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.put(nestedBerkeleyTxn, binaryKey, binaryTxn))
                }

                scala.concurrent.blocking(producerTransactionsDatabase.put(nestedBerkeleyTxn, binaryKey, binaryTxn, Put.OVERWRITE, new WriteOptions().setTTL(calculateTTLForBerkeleyRecord(dbStream.ttl))))
              }
            case _ =>
              val ProducerTransactionWithNewState = transiteProducerTransactionToNewState(txns)

              val binaryTxn = ProducerTransactionWithNewState.producerTransaction.toDatabaseEntry
              val binaryKey = key.toDatabaseEntry

              if (ProducerTransactionWithNewState.state == TransactionStates.Opened) {
                scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.put(nestedBerkeleyTxn, binaryKey, binaryTxn))
              }

              scala.concurrent.blocking(producerTransactionsDatabase.put(nestedBerkeleyTxn, binaryKey, binaryTxn, Put.OVERWRITE, new WriteOptions().setTTL(calculateTTLForBerkeleyRecord(dbStream.ttl))))
          }
        }
      }
    }

//    val isOkay = scala.util.Try(nestedBerkeleyTxn.commit()) match {
//      case scala.util.Success(_) => true
//      case scala.util.Failure(_) => false
//    }

//    if (isOkay)
      putConsumerTransactions(decomposeConsumerTransactionsToDatabaseRepresentation(consumerTransactions), parentBerkeleyTxn)
//    else
//      isOkay
    true
  }

  class BigCommit {
    private val transactionDB = transactionMetaEnviroment.beginTransaction(null, null)
    def putSomeTransactions(transactions: Seq[(Transaction, Long)]) = putTransactions(transactions, transactionDB)

    def commit(): Boolean = scala.util.Try(transactionDB.commit(defaultDurability)) match {
      case scala.util.Success(_) => true
      case scala.util.Failure(_) => false
    }

    def abort(): Boolean  = scala.util.Try(transactionDB.abort()) match {
      case scala.util.Success(_) => true
      case scala.util.Failure(_) => false
    }
  }

  def getBigCommit = new BigCommit()

  private final val comparator = UnsignedBytes.lexicographicalComparator
  def scanTransactions(token: Int, stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[Transaction]] =
    authenticate(token) {
      val lockMode = LockMode.READ_UNCOMMITTED_ALL
      val streamObj = getStreamDatabaseObject(stream)
      val transactionDB = transactionMetaEnviroment.beginTransaction(null, null)
      val cursor = producerTransactionsDatabase.openCursor(transactionDB, null)

      def producerTransactionToTransaction(txn: ProducerTransactionKey) = {
        val producerTxn = transactionService.rpc.ProducerTransaction(streamObj.name, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.keepAliveTTL)
        Transaction(Some(producerTxn), None)
      }

      def moveCursorToKey: Option[ProducerTransactionKey] = {
        val keyFrom = new Key(streamObj.streamNameToLong, partition, long2Long(from))
        val keyFound = keyFrom.toDatabaseEntry
        val dataFound = new DatabaseEntry()
        if (cursor.getSearchKey(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS)
          Some(new ProducerTransactionKey(keyFrom, ProducerTransactionWithoutKey.entryToObject(dataFound))) else None
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
            txns += ProducerTransactionKey(Key.entryToObject(keyFound), ProducerTransactionWithoutKey.entryToObject(dataFound))
          }

          cursor.close()
          transactionDB.commit()

          txns map producerTransactionToTransaction
      }
    }(executionContext.berkeleyReadContext)

//  private val markTransactionsAsInvalid = new Runnable {
//    logger.debug(s"Cleaner of expired transactions is running.")
//    val cleanAmountPerDatabaseTransaction = storageOpts.clearAmount
//    val lockMode = LockMode.READ_UNCOMMITTED_ALL
//
//    override def run(): Unit = {
//      val transactionDB = transactionMetaEnviroment.beginTransaction(null, null)
//      val cursorProducerTransactionsOpened = producerTransactionsWithOpenedStateDatabase.openCursor(transactionDB, null)
//
//
//      def deleteTransactionIfExpired(cursor: Cursor): Boolean = {
//        val keyFound = new DatabaseEntry()
//        val dataFound = new DatabaseEntry()
//
//        if (cursor.getNext(keyFound, dataFound, lockMode) == OperationStatus.SUCCESS) {
//          val producerTransaction = ProducerTransactionWithoutKey.entryToObject(dataFound)
//          val toDelete: Boolean = ??? //doesProducerTransactionExpired(producerTransaction)
//          if (toDelete) {
//            logger.debug(s"Cleaning $producerTransaction as it's expired.")
//            cursor.delete()
//            true
//          } else true
//        } else false
//      }
//
//
//      @tailrec
//      def repeat(counter: Int, cursor: Cursor): Unit = {
//        val doesExistAnyTransactionToDelete = deleteTransactionIfExpired(cursor)
//        if (counter > 0 && doesExistAnyTransactionToDelete) repeat(counter - 1, cursor)
//        else cursor.close()
//      }
//
//      repeat(cleanAmountPerDatabaseTransaction, cursorProducerTransactionsOpened)
//      transactionDB.commit()
//    }
//  }
//
//  def closeTransactionMetaDatabases(): Unit = {
//    Option(producerTransactionsDatabase.close())
//    Option(producerTransactionsWithOpenedStateDatabase.close())
//  }
//
//  def closeTransactionMetaEnviroment() = {
//    Option(transactionMetaEnviroment.close())
//  }

//  scheduledExecutor.scheduleWithFixedDelay(markTransactionsAsInvalid, 0, storageOpts.clearDelayMs, java.util.concurrent.TimeUnit.SECONDS)
}
