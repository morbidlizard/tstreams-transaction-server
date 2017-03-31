package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionKey
import com.bwsw.tstreamstransactionserver.netty.server.streamService.KeyStream
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{KeyStreamPartition, LastTransactionStreamPartition, TransactionStateHandler}
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, StreamCache}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.rpc._
import com.sleepycat.je._
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future => ScalaFuture}

trait  TransactionMetaServiceImpl extends TransactionStateHandler with StreamCache with LastTransactionStreamPartition
  with Authenticable
  with ProducerTransactionStateNotifier
{
  def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionKey], parentBerkeleyTxn: com.sleepycat.je.Transaction): Unit

  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val environment: Environment

  val producerTransactionsDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = "TransactionStore"//storageOpts.metadataStorageName
    environment.openDatabase(null, storeName, dbConfig)
  }

  val producerTransactionsWithOpenedStateDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = "TransactionOpenStore"//storageOpts.openedTransactionsStorageName
    environment.openDatabase(null, storeName, dbConfig)
  }

  private def fillOpenedTransactionsRAMTable: com.google.common.cache.Cache[Key, ProducerTransactionWithoutKey] = {
    if (logger.isDebugEnabled) logger.debug("Filling cache with Opened Transactions table.")
    val secondsToLive = 180
    val threadsToWriteNumber = 1
    val cache = com.google.common.cache.CacheBuilder.newBuilder()
      .concurrencyLevel(threadsToWriteNumber)
      .expireAfterAccess(secondsToLive, TimeUnit.SECONDS)
      .build[Key, ProducerTransactionWithoutKey]()

    val keyFound = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val cursor = producerTransactionsWithOpenedStateDatabase.openCursor(new DiskOrderedCursorConfig())
    while (cursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      cache.put(Key.entryToObject(keyFound), ProducerTransactionWithoutKey.entryToObject(dataFound))
    }
    cursor.close()

    cache
  }

  private val transactionsRamTable: com.google.common.cache.Cache[Key, ProducerTransactionWithoutKey] = fillOpenedTransactionsRAMTable

  protected def getOpenedTransaction(key: Key): Option[ProducerTransactionWithoutKey] = {
    val transaction = transactionsRamTable.getIfPresent(key)
    if (transaction != null) Some(transaction)
    else {
      val keyFound = key.toDatabaseEntry
      val dataFound = new DatabaseEntry()

      if (producerTransactionsWithOpenedStateDatabase.get(null, keyFound, dataFound, null) == OperationStatus.SUCCESS) {
        val transactionOpt = ProducerTransactionWithoutKey.entryToObject(dataFound)
        transactionsRamTable.put(key, transactionOpt)
        Some(transactionOpt)
      }
      else
        None
    }
  }

  private type Timestamp = Long

  private final def decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions: Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Timestamp)], berkeleyTransaction: com.sleepycat.je.Transaction) = {
    if (logger.isDebugEnabled) logger.debug("Decomposing transactions to producer and consumer transactions")

    val producerTransactions = ArrayBuffer[(ProducerTransaction, Timestamp)]()
    val consumerTransactions = ArrayBuffer[(ConsumerTransaction, Timestamp)]()

    transactions foreach { case (transaction, timestamp) =>
      (transaction.producerTransaction, transaction.consumerTransaction) match {
        case (Some(txn), _) =>
          //even if producer transaction is belonged to deleted stream we can omit such transaction, as client shouldn't see it.
          scala.util.Try(getMostRecentStream(txn.stream)) match {
            case scala.util.Success(recentStream) =>
              val key = KeyStreamPartition(recentStream.streamNameToLong, txn.partition)
              if (txn.state != TransactionStates.Opened) {
                producerTransactions += ((txn, timestamp))
              } else if (!isThatTransactionOutOfOrder(key, txn.transactionID)) {
                // updating RAM table, and last opened transaction database.
                updateLastTransactionStreamPartitionRamTable(key, txn.transactionID, isOpenedTransaction = true)
                putLastTransaction(key, txn.transactionID, isOpenedTransaction = true, berkeleyTransaction)
                if (logger.isDebugEnabled) logger.debug(s"On stream:${key.stream} partition:${key.partition} last opened transaction is ${txn.transactionID} now.")
                producerTransactions += ((txn, timestamp))
              }
            case _ => //It doesn't matter
          }

        case (_, Some(txn)) =>
          scala.util.Try(getMostRecentStream(txn.stream)) match {
            //even if consumer transaction is belonged to deleted stream we can omit such transaction, as client shouldn't see it.
            case scala.util.Success(recentStream) =>
              val key = KeyStreamPartition(recentStream.streamNameToLong, txn.partition)
              consumerTransactions += ((txn, timestamp))
            case _ => //It doesn't matter
          }
        case _ =>
      }
    }
    (producerTransactions, consumerTransactions)
  }

  private final def groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(txns: Seq[(ProducerTransaction, Timestamp)], berkeleyTransaction: com.sleepycat.je.Transaction): Map[KeyStream, ArrayBuffer[ProducerTransactionKey]] = {
    if (logger.isDebugEnabled) logger.debug("Mapping all producer transactions streams attrbute to long representation(ID), grouping them by stream and partition, checking that the stream isn't deleted in order to process producer transactions.")
    txns.foldLeft[scala.collection.mutable.Map[KeyStream, ArrayBuffer[ProducerTransactionKey]]](scala.collection.mutable.Map()) { case (acc, (producerTransaction, timestamp)) =>
      val keyStreams = getStreamFromOldestToNewest(producerTransaction.stream)
      //it's okay that one looking for a stream that correspond to the transaction, as client can create/delete new streams.
      val streamForThisTransaction = keyStreams.filter(_.stream.timestamp <= timestamp).lastOption
      streamForThisTransaction match {
        case Some(keyStream) if !keyStream.stream.deleted =>
          if (acc.contains(keyStream))
            acc(keyStream) += ProducerTransactionKey(producerTransaction, keyStream.streamNameToLong, timestamp)
          else
            acc += ((keyStream, ArrayBuffer(ProducerTransactionKey(producerTransaction, keyStream.streamNameToLong, timestamp))))

          acc
        case _ => acc
      }
    }.toMap
  }


  private final def decomposeConsumerTransactionsToDatabaseRepresentation(transactions: Seq[(ConsumerTransaction, Timestamp)]) = {
    val consumerTransactionsKey = ArrayBuffer[ConsumerTransactionKey]()
    transactions foreach { case (txn, timestamp) => scala.util.Try {
      //it's okay that one looking for a stream that correspond to the transaction, as client can create/delete new streams.
      val streamForThisTransactionOpt = getStreamFromOldestToNewest(txn.stream).filter(_.stream.timestamp <= timestamp).lastOption
      streamForThisTransactionOpt foreach (streamForThisTransaction => consumerTransactionsKey += ConsumerTransactionKey(txn, streamForThisTransaction.streamNameToLong, timestamp))
    }
    }
    consumerTransactionsKey
  }

  private final def groupProducerTransactions(producerTransactions: Seq[ProducerTransactionKey]) = producerTransactions.toArray.groupBy(txn => txn.key)


  private final def calculateTTLForBerkeleyRecord(ttl: Long) = {
    val convertedTTL = TimeUnit.SECONDS.toHours(ttl)
    if (convertedTTL == 0L) 1 else scala.math.abs(convertedTTL.toInt)
  }

  private final def updateLastCheckpointedTransactionAndPutToDatabase(key: stateHandler.KeyStreamPartition, producerTransactionWithNewState: ProducerTransactionKey, parentBerkeleyTxn: com.sleepycat.je.Transaction): Unit = {
    updateLastTransactionStreamPartitionRamTable(key, producerTransactionWithNewState.transactionID, isOpenedTransaction = false)
    putLastTransaction(key, producerTransactionWithNewState.transactionID, isOpenedTransaction = false, parentBerkeleyTxn)
    if (logger.isDebugEnabled())
      logger.debug(s"On stream:${key.stream} partition:${key.partition} last checkpointed transaction is ${producerTransactionWithNewState.transactionID} now.")
  }

  private def putTransactions(transactions: Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Long)], berkeleyTransaction: com.sleepycat.je.Transaction): Unit = {
    val (producerTransactions, consumerTransactions) = decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions, berkeleyTransaction)
    val groupedProducerTransactionsWithTimestamp = groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(producerTransactions, berkeleyTransaction)

    groupedProducerTransactionsWithTimestamp.foreach { case (stream, dbProducerTransactions) =>

      val groupedProducerTransactions = groupProducerTransactions(dbProducerTransactions)

      groupedProducerTransactions foreach { case (key, txns) =>
        //retrieving an opened transaction from opened transaction database if it exist
        val openedTransactionOpt = getOpenedTransaction(key)
        val producerTransactionWithNewState = scala.util.Try(openedTransactionOpt match {
          case Some(data) =>
            val persistedProducerTransactionBerkeley = ProducerTransactionKey(key, data)
            if (logger.isDebugEnabled) logger.debug(s"Transiting producer transaction on stream: ${persistedProducerTransactionBerkeley.stream}" +
              s"partition ${persistedProducerTransactionBerkeley.partition}, transaction ${persistedProducerTransactionBerkeley.transactionID} " +
              s"with state ${persistedProducerTransactionBerkeley.state} to new state")
            transitProducerTransactionToNewState(persistedProducerTransactionBerkeley, txns)
          case None =>
            if (logger.isDebugEnabled) logger.debug(s"Trying to put new producer transaction on stream ${key.stream}.")
            transitProducerTransactionToNewState(txns)
        })


        producerTransactionWithNewState match {
          case scala.util.Success(producerTransactionKey) =>
            val binaryTxn = producerTransactionKey.producerTransaction.toDatabaseEntry
            val binaryKey = key.toDatabaseEntry

            if (producerTransactionKey.state == TransactionStates.Checkpointed) {
              updateLastCheckpointedTransactionAndPutToDatabase(stateHandler.KeyStreamPartition(key.stream, key.partition), producerTransactionKey, berkeleyTransaction)
            }

            transactionsRamTable.put(producerTransactionKey.key, producerTransactionKey.producerTransaction)
            if (producerTransactionKey.state == TransactionStates.Opened) {
              scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.put(berkeleyTransaction, binaryKey, binaryTxn))
            } else {
              scala.concurrent.blocking(producerTransactionsWithOpenedStateDatabase.delete(berkeleyTransaction, binaryKey))
            }
            if (areThereAnyNotifies)
              tryCompleteNotify(producerTransactionKey)

            scala.concurrent.blocking(producerTransactionsDatabase.put(berkeleyTransaction, binaryKey, binaryTxn, Put.OVERWRITE, new WriteOptions().setTTL(calculateTTLForBerkeleyRecord(stream.ttl))))
          case scala.util.Failure(throwable) =>
          //throwable.printStackTrace()
        }
      }
    }
    putConsumerTransactions(decomposeConsumerTransactionsToDatabaseRepresentation(consumerTransactions), berkeleyTransaction)
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

    def putSomeTransactions(transactions: Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Long)]): Unit = {
      if (logger.isDebugEnabled) logger.debug("Adding to commit new transactions from commit log file.")
      putTransactions(transactions, transactionDB)
    }

    def commit(fileCreationTimestamp: Long): Boolean = {
      val timestampCommitLog = TimestampCommitLog(fileCreationTimestamp, pathToFile)
      commitLogDatabase.putNoOverwrite(transactionDB, timestampCommitLog.keyToDatabaseEntry, timestampCommitLog.dataToDatabaseEntry)
      scala.util.Try(transactionDB.commit()) match {
        case scala.util.Success(_) => true
        case scala.util.Failure(_) => false
      }
    }

    def abort(): Boolean = scala.util.Try(transactionDB.abort()) match {
      case scala.util.Success(_) => true
      case scala.util.Failure(_) => false
    }
  }

  def getBigCommit(pathToFile: String) = new BigCommit(pathToFile)


  def getTransaction(stream: String, partition: Int, transaction: Long): ScalaFuture[com.bwsw.tstreamstransactionserver.rpc.TransactionInfo] = {
    val keyStream = getMostRecentStream(stream)
    val lastTransaction = getLastTransactionIDAndCheckpointedID(keyStream.streamNameToLong, partition)
    if (lastTransaction.isEmpty || transaction > lastTransaction.get.opened.id) {
      ScalaFuture.successful(TransactionInfo(exists = false, None))
    } else {
      ScalaFuture {
        val searchKey = new Key(keyStream.streamNameToLong, partition, transaction).toDatabaseEntry
        val searchData = new DatabaseEntry()

        val transactionDB = environment.beginTransaction(null, null)
        val operationStatus = producerTransactionsDatabase.get(transactionDB, searchKey, searchData, null)
        val maybeProducerTransactionKey = if (operationStatus == OperationStatus.SUCCESS)
          Some(new ProducerTransactionKey(Key.entryToObject(searchKey), ProducerTransactionWithoutKey.entryToObject(searchData))) else None

        maybeProducerTransactionKey match {
          case None =>
            transactionDB.commit()
            TransactionInfo(exists = true, None)

          case Some(producerTransactionKey) =>
            transactionDB.commit()

            TransactionInfo(exists = true, Some(keyToProducerTransaction(producerTransactionKey, keyStream.name)))
        }
      }(executionContext.berkeleyReadContext)
    }
  }

  final def getLastCheckpoitnedTransaction(stream: String, partition: Int): ScalaFuture[Option[Long]] = ScalaFuture{
    val keyStream = getMostRecentStream(stream)
    getLastTransactionIDAndCheckpointedID(keyStream.streamNameToLong, partition) match {
      case Some(last) => last.checkpointed match {
        case Some(checkpointed) => Some(checkpointed.id)
        case None => None
      }
      case None => None
    }
  }(executionContext.berkeleyReadContext)

  def scanTransactions(stream: String, partition: Int, from: Long, to: Long, lambda: ProducerTransaction => Boolean = txn => true): ScalaFuture[com.bwsw.tstreamstransactionserver.rpc.ScanTransactionsInfo] =
    ScalaFuture {
      val lockMode = LockMode.READ_UNCOMMITTED_ALL
      val keyStream = getMostRecentStream(stream)
      val transactionDB = environment.beginTransaction(null, null)
      val cursor = producerTransactionsDatabase.openCursor(transactionDB, new CursorConfig().setNonSticky(true))

      def producerTransactionKeyToProducerTransaction(txn: ProducerTransactionKey) = {
        com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(keyStream.name, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.ttl)
      }

      val (lastOpenedTransactionID, toTransactionID) = getLastTransactionIDAndCheckpointedID(keyStream.streamNameToLong, partition) match {
        case Some(lastTransaction) => lastTransaction.opened.id match {
          case lt if lt < from => (lt, from - 1L)
          case lt if from <= lt && lt < to => (lt, lt)
          case lt if lt >= to => (lt, to)
        }
        case None => (-1L, from - 1L)
      }

      if (logger.isDebugEnabled) logger.debug(s"Trying to retrieve transactions on stream $stream, partition: $partition in range [$from, $to]." +
        s"Actually as lt ${if (lastOpenedTransactionID == -1) "doesn't exist" else s"is $lastOpenedTransactionID"} the range is [$from, $toTransactionID].")

      if (toTransactionID < from) ScanTransactionsInfo(lastOpenedTransactionID, Seq())
      else {
        val lastTransactionID = new Key(keyStream.streamNameToLong, partition, toTransactionID).toDatabaseEntry
        def moveCursorToKey: Option[ProducerTransactionKey] = {
          val keyFrom = new Key(keyStream.streamNameToLong, partition, from)
          val keyFound = keyFrom.toDatabaseEntry
          val dataFound = new DatabaseEntry()
          val toStartFrom = cursor.getSearchKeyRange(keyFound, dataFound, lockMode)
          if (toStartFrom == OperationStatus.SUCCESS && producerTransactionsDatabase.compareKeys(keyFound, lastTransactionID) <= 0)
            Some(new ProducerTransactionKey(Key.entryToObject(keyFound), ProducerTransactionWithoutKey.entryToObject(dataFound)))
          else None
        }

        moveCursorToKey match {
          case None =>
            cursor.close()
            transactionDB.commit()
            ScanTransactionsInfo(lastOpenedTransactionID, Seq())

          case Some(producerTransactionKey) =>
            val producerTransactions = ArrayBuffer[ProducerTransactionKey](producerTransactionKey)
            val transactionID = new DatabaseEntry()
            val dataFound = new DatabaseEntry()

            //return transactions until first opened one.
            var txnState: TransactionStates = producerTransactionKey.state
            while (
              cursor.getNext(transactionID, dataFound, lockMode) == OperationStatus.SUCCESS &&
                (producerTransactionsDatabase.compareKeys(transactionID, lastTransactionID) <= 0 && txnState != TransactionStates.Opened)
            ) {
              val producerTransaction = ProducerTransactionKey(Key.entryToObject(transactionID), ProducerTransactionWithoutKey.entryToObject(dataFound))
              txnState = producerTransaction.state
              producerTransactions += producerTransaction
            }

            cursor.close()
            transactionDB.commit()

            ScanTransactionsInfo(lastOpenedTransactionID, producerTransactions map producerTransactionKeyToProducerTransaction filter lambda)
        }
      }
    }(executionContext.berkeleyReadContext)

  private def keyToProducerTransaction(txn: ProducerTransactionKey, stream: String) = {
    com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(stream, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.ttl)
  }

  final class TransactionsToDeleteTask(timestampToDeleteTransactions: Long) extends Runnable {
    private val lockMode = LockMode.READ_UNCOMMITTED_ALL

    private def doesProducerTransactionExpired(producerTransactionWithoutKey: ProducerTransactionWithoutKey): Boolean = {
      scala.math.abs(producerTransactionWithoutKey.timestamp + TimeUnit.SECONDS.toMillis(producerTransactionWithoutKey.ttl)) <= timestampToDeleteTransactions
    }

    private def transitToInvalidState(producerTransactionWithoutKey: ProducerTransactionWithoutKey) = {
      ProducerTransactionWithoutKey(TransactionStates.Invalid, producerTransactionWithoutKey.quantity, 0L, timestampToDeleteTransactions)
    }

    override def run(): Unit = {
      if (logger.isDebugEnabled) logger.debug(s"Cleaner[time: $timestampToDeleteTransactions] of expired transactions is running.")
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
            transactionsRamTable.invalidate(Key.entryToObject(keyFound))
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

  def closeTransactionMetaDatabases(): Unit = {
    scala.util.Try(producerTransactionsDatabase.close())
    scala.util.Try(producerTransactionsWithOpenedStateDatabase.close())
  }

  def closeTransactionMetaEnvironment(): Unit = {
    scala.util.Try(environment.close())
  }
}
