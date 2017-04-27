package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.util.concurrent.{Callable, TimeUnit}

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{Batch, RocksDBALL}
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamRecord
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{KeyStreamPartition, LastTransactionStreamPartition, TransactionStateHandler}
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, HasEnvironment, StreamCache}
import com.bwsw.tstreamstransactionserver.rpc._

import org.rocksdb.RocksIterator
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future => ScalaFuture}

trait TransactionMetaServiceImpl extends TransactionStateHandler with StreamCache with LastTransactionStreamPartition
  with Authenticable
  with ProducerTransactionStateNotifier
{

  def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord], batch: Batch): Unit
  val executionContext: ServerExecutionContext
  val rocksMetaServiceDB: RocksDBALL

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val producerTransactionsDatabase = rocksMetaServiceDB.getDatabase(HasEnvironment.TRANSACTION_ALL_STORE)
  private val producerTransactionsWithOpenedStateDatabase = rocksMetaServiceDB.getDatabase(HasEnvironment.TRANSACTION_OPEN_STORE)

  private def fillOpenedTransactionsRAMTable: com.google.common.cache.Cache[ProducerTransactionKey, ProducerTransactionValue] = {
    if (logger.isDebugEnabled) logger.debug("Filling cache with Opened Transactions table.")
    val secondsToLive = 180
    val threadsToWriteNumber = 1
    val cache = com.google.common.cache.CacheBuilder.newBuilder()
      .concurrencyLevel(threadsToWriteNumber)
      .expireAfterAccess(secondsToLive, TimeUnit.SECONDS)
      .build[ProducerTransactionKey, ProducerTransactionValue]()

    val iterator = producerTransactionsWithOpenedStateDatabase.iterator
    iterator.seekToFirst()
    while (iterator.isValid) {
      cache.put(ProducerTransactionKey.fromByteArray(iterator.key()), ProducerTransactionValue.fromByteArray(iterator.value()))
      iterator.next()
    }
    iterator.close()

    cache
  }

  private val transactionsRamTable: com.google.common.cache.Cache[ProducerTransactionKey, ProducerTransactionValue] = fillOpenedTransactionsRAMTable

  protected def getOpenedTransaction(key: ProducerTransactionKey): Option[ProducerTransactionValue] = {
    val transaction = Option(transactionsRamTable.getIfPresent(key))
    if (transaction.isDefined) transaction
    else {
      val keyFound = key.toByteArray
      Option(producerTransactionsWithOpenedStateDatabase.get(keyFound)).map{data =>
        val producerTransactionValue = ProducerTransactionValue.fromByteArray(data)
        transactionsRamTable.put(key, producerTransactionValue)
        producerTransactionValue
      }
    }
  }

  private type Timestamp = Long

  private final def decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions: Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Timestamp)], batch: Batch) = {
    if (logger.isDebugEnabled) logger.debug("Decomposing transactions to producer and consumer transactions")

    val producerTransactions = ArrayBuffer[(ProducerTransaction, Timestamp)]()
    val consumerTransactions = ArrayBuffer[(ConsumerTransaction, Timestamp)]()

    transactions foreach { case (transaction, timestamp) =>
      (transaction.producerTransaction, transaction.consumerTransaction) match {
        case (Some(txn), _) =>
          //even if producer transaction is belonged to deleted stream we can omit such transaction, as client shouldn't see it.
          scala.util.Try(getMostRecentStream(txn.stream)) match {
            case scala.util.Success(recentStream) =>
              val key = KeyStreamPartition(recentStream.id, txn.partition)
              if (txn.state != TransactionStates.Opened) {
                producerTransactions += ((txn, timestamp))
              } else if (!isThatTransactionOutOfOrder(key, txn.transactionID)) {
                // updating RAM table, and last opened transaction database.
                updateLastTransactionStreamPartitionRamTable(key, txn.transactionID, isOpenedTransaction = true)
                putLastTransaction(key, txn.transactionID, isOpenedTransaction = true, batch)
                if (logger.isDebugEnabled) logger.debug(s"On stream:${key.stream} partition:${key.partition} last opened transaction is ${txn.transactionID} now.")
                producerTransactions += ((txn, timestamp))
              }
            case _ => //It doesn't matter
          }

        case (_, Some(txn)) =>
          scala.util.Try(getMostRecentStream(txn.stream)) match {
            //even if consumer transaction is belonged to deleted stream we can omit such transaction, as client shouldn't see it.
            case scala.util.Success(recentStream) =>
              val key = KeyStreamPartition(recentStream.id, txn.partition)
              consumerTransactions += ((txn, timestamp))
            case _ => //It doesn't matter
          }
        case _ =>
      }
    }
    (producerTransactions, consumerTransactions)
  }

  private final def groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(txns: Seq[(ProducerTransaction, Timestamp)]): Map[StreamRecord, ArrayBuffer[ProducerTransactionRecord]] = {
    if (logger.isDebugEnabled) logger.debug("Mapping all producer transactions streams attrbute to long representation(ID), grouping them by stream and partition, checking that the stream isn't deleted in order to process producer transactions.")
    txns.foldLeft[scala.collection.mutable.Map[StreamRecord, ArrayBuffer[ProducerTransactionRecord]]](scala.collection.mutable.Map()) { case (acc, (producerTransaction, timestamp)) =>
      val keyRecords = getStreamFromOldestToNewest(producerTransaction.stream)
      //it's okay that one looking for a stream that correspond to the transaction, as client can create/delete new streams.
      val streamForThisTransaction = keyRecords.filter(_.stream.timestamp <= timestamp).lastOption
      streamForThisTransaction match {
        case Some(keyStream) if !keyStream.stream.deleted =>
          if (acc.contains(keyStream))
            acc(keyStream) += ProducerTransactionRecord(producerTransaction, keyStream.id, timestamp)
          else
            acc += ((keyStream, ArrayBuffer(ProducerTransactionRecord(producerTransaction, keyStream.id, timestamp))))

          acc
        case _ => acc
      }
    }.toMap
  }


  private final def decomposeConsumerTransactionsToDatabaseRepresentation(transactions: Seq[(ConsumerTransaction, Timestamp)]) = {
    val consumerTransactionsKey = ArrayBuffer[ConsumerTransactionRecord]()
    transactions foreach { case (txn, timestamp) => scala.util.Try {
      //it's okay that one looking for a stream that correspond to the transaction, as client can create/delete new streams.
      val streamForThisTransactionOpt = getStreamFromOldestToNewest(txn.stream).filter(_.stream.timestamp <= timestamp).lastOption
      streamForThisTransactionOpt foreach (streamForThisTransaction => consumerTransactionsKey += ConsumerTransactionRecord(txn, streamForThisTransaction.id, timestamp))
    }
    }
    consumerTransactionsKey
  }

  private final def groupProducerTransactions(producerTransactions: Seq[ProducerTransactionRecord]) = producerTransactions.toArray.groupBy(txn => txn.key)

  private final def updateLastCheckpointedTransactionAndPutToDatabase(key: stateHandler.KeyStreamPartition, producerTransactionWithNewState: ProducerTransactionRecord, batch: Batch): Unit = {
    updateLastTransactionStreamPartitionRamTable(key, producerTransactionWithNewState.transactionID, isOpenedTransaction = false)
    putLastTransaction(key, producerTransactionWithNewState.transactionID, isOpenedTransaction = false, batch)
    if (logger.isDebugEnabled())
      logger.debug(s"On stream:${key.stream} partition:${key.partition} last checkpointed transaction is ${producerTransactionWithNewState.transactionID} now.")
  }

  private def putTransactions(transactions: Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Long)], batch: Batch): Unit = {
    val (producerTransactions, consumerTransactions) = decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions, batch)
    val groupedProducerTransactionsWithTimestamp = groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(producerTransactions)

    groupedProducerTransactionsWithTimestamp.foreach { case (stream, dbProducerTransactions) =>
      val groupedProducerTransactions = groupProducerTransactions(dbProducerTransactions)
      groupedProducerTransactions foreach { case (key, txns) =>
        //retrieving an opened transaction from opened transaction database if it exist
        val openedTransactionOpt = getOpenedTransaction(key)
        val producerTransactionWithNewState = scala.util.Try(openedTransactionOpt match {
          case Some(data) =>
            val persistedProducerTransactionRocks = ProducerTransactionRecord(key, data)
            if (logger.isDebugEnabled) logger.debug(s"Transiting producer transaction on stream: ${persistedProducerTransactionRocks.stream}" +
              s"partition ${persistedProducerTransactionRocks.partition}, transaction ${persistedProducerTransactionRocks.transactionID} " +
              s"with state ${persistedProducerTransactionRocks.state} to new state")
            transitProducerTransactionToNewState(persistedProducerTransactionRocks, txns)
          case None =>
            if (logger.isDebugEnabled) logger.debug(s"Trying to put new producer transaction on stream ${key.stream}.")
            transitProducerTransactionToNewState(txns)
        })

        producerTransactionWithNewState match {
          case scala.util.Success(producerTransactionRecord) =>
            val binaryTxn = producerTransactionRecord.producerTransaction.toByteArray
            val binaryKey = key.toByteArray

            if (producerTransactionRecord.state == TransactionStates.Checkpointed) {
              updateLastCheckpointedTransactionAndPutToDatabase(stateHandler.KeyStreamPartition(key.stream, key.partition), producerTransactionRecord, batch)
            }

            transactionsRamTable.put(producerTransactionRecord.key, producerTransactionRecord.producerTransaction)
            if (producerTransactionRecord.state == TransactionStates.Opened) {
              batch.put(HasEnvironment.TRANSACTION_OPEN_STORE, binaryKey, binaryTxn)
            }
            else {
              batch.remove(HasEnvironment.TRANSACTION_OPEN_STORE, binaryKey)
            }
            if (areThereAnyProducerNotifies)
              tryCompleteProducerNotify(producerTransactionRecord)

            batch.put(HasEnvironment.TRANSACTION_ALL_STORE, binaryKey, binaryTxn)
            if (logger.isDebugEnabled) logger.debug(s"Producer transaction on stream: ${producerTransactionRecord.stream}" +
              s"partition ${producerTransactionRecord.partition}, transactionId ${producerTransactionRecord.transactionID} " +
              s"with state ${producerTransactionRecord.state} is ready for commit[commit id: ${batch.id}]"
            )
          case scala.util.Failure(throwable) => //throwable.printStackTrace()
        }
      }
    }
    val consumerTransactionsToProcess = decomposeConsumerTransactionsToDatabaseRepresentation(consumerTransactions)
    if (consumerTransactionsToProcess.nonEmpty) putConsumerTransactions(consumerTransactionsToProcess, batch)
  }


  private val commitLogDatabase = rocksMetaServiceDB.getDatabase(HasEnvironment.COMMIT_LOG_STORE)

  private[server] final def getLastProcessedCommitLogFileID: Option[Long] = {
    val iterator = commitLogDatabase.iterator
    iterator.seekToLast()

    val id = if (iterator.isValid)
      Some(CommitLogKey.fromByteArray(iterator.key()).id)
    else
      None

    iterator.close()
    id
  }

  private[server] final def getProcessedCommitLogFiles: ArrayBuffer[Long] = {
    val processedCommitLogFiles = scala.collection.mutable.ArrayBuffer[Long]()

    val iterator = commitLogDatabase.iterator
    iterator.seekToFirst()

    while (iterator.isValid) {
      processedCommitLogFiles += CommitLogKey.fromByteArray(iterator.key()).id
      iterator.next()
    }
    iterator.close()

    processedCommitLogFiles
  }

  final class BigCommit(fileID: Long) {
    private val batch = rocksMetaServiceDB.newBatch

    private class Commit extends Callable[Boolean] {
      override def call(): Boolean = {
        val key   = CommitLogKey(fileID).toByteArray
        val value = Array[Byte]()

        batch.put(HasEnvironment.COMMIT_LOG_STORE, key, value)
        if (batch.write()) {
          if (logger.isDebugEnabled) logger.debug(s"commit ${batch.id} is successfully fixed.")
          true
        } else {
          false
        }
      }
    }

//    private class Abort extends Callable[Boolean] {
//      override def call(): Boolean =
//        scala.util.Try(transactionDB.abort()) match {
//          case scala.util.Success(_) => true
//          case scala.util.Failure(error) => throw error
//        }
//    }

    private class PutTransactions(transactions: Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Long)]) extends Callable[Unit] {
      if (logger.isDebugEnabled) logger.debug("Adding to commit new transactions from commit log file.")
      override def call(): Unit = putTransactions(transactions, batch)
    }

    def putSomeTransactions(transactions: Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Long)]): Unit = {
      executionContext.berkeleyWriteContext.submit(new PutTransactions(transactions)).get()
    }

    def commit(): Boolean = {
      executionContext.berkeleyWriteContext.submit(new Commit()).get()
    }

//    def abort(): Boolean = {
//      executionContext.berkeleyWriteContext.submit(new Abort()).get()
//    }
  }

  def getBigCommit(fileID: Long) = new BigCommit(fileID)


  final def getTransaction(stream: String, partition: Int, transaction: Long): ScalaFuture[com.bwsw.tstreamstransactionserver.rpc.TransactionInfo] = ScalaFuture {
    val keyStream = getMostRecentStream(stream)
    val lastTransaction = getLastTransactionIDAndCheckpointedID(keyStream.id, partition)
    if (lastTransaction.isEmpty || transaction > lastTransaction.get.opened.id) {
      TransactionInfo(exists = false, None)
    } else {
      val searchKey = new ProducerTransactionKey(keyStream.id, partition, transaction).toByteArray

      Option(producerTransactionsDatabase.get(searchKey)).map(searchData =>
        new ProducerTransactionRecord(ProducerTransactionKey.fromByteArray(searchKey), ProducerTransactionValue.fromByteArray(searchData))
      ) match {
        case None =>
          TransactionInfo(exists = true, None)
        case Some(producerTransactionRecord) =>
          TransactionInfo(exists = true, Some(recordToProducerTransaction(producerTransactionRecord, keyStream.name)))
      }
    }
  }(executionContext.berkeleyReadContext)

  final def getLastCheckpointedTransaction(stream: String, partition: Int): ScalaFuture[Option[Long]] = ScalaFuture{
    val streamRecord = getMostRecentStream(stream)
    val result = getLastTransactionIDAndCheckpointedID(streamRecord.id, partition) match {
      case Some(last) => last.checkpointed match {
        case Some(checkpointed) => Some(checkpointed.id)
        case None => None
      }
      case None => None
    }
    result
  }(executionContext.berkeleyReadContext)

  private val comparator = com.bwsw.tstreamstransactionserver.`implicit`.Implicits.ByteArray
  def scanTransactions(stream: String, partition: Int, from: Long, to: Long, count: Int, states: collection.Set[TransactionStates]): ScalaFuture[com.bwsw.tstreamstransactionserver.rpc.ScanTransactionsInfo] =
    ScalaFuture {
      val keyStream = getMostRecentStream(stream)

      val (lastOpenedTransactionID, toTransactionID) = getLastTransactionIDAndCheckpointedID(keyStream.id, partition) match {
        case Some(lastTransaction) => lastTransaction.opened.id match {
          case lt if lt < from => (lt, from - 1L)
          case lt if from <= lt && lt < to => (lt, lt)
          case lt if lt >= to => (lt, to)
        }
        case None => (-1L, from - 1L)
      }

      if (logger.isDebugEnabled) logger.debug(s"Trying to retrieve transactions on stream $stream, partition: $partition in range [$from, $to]." +
        s"Actually as lt ${if (lastOpenedTransactionID == -1) "doesn't exist" else s"is $lastOpenedTransactionID"} the range is [$from, $toTransactionID].")

      if (toTransactionID < from || count == 0) ScanTransactionsInfo(lastOpenedTransactionID, Seq())
      else {
        val iterator = producerTransactionsDatabase.iterator


        val lastTransactionID = new ProducerTransactionKey(keyStream.id, partition, toTransactionID).toByteArray
        def moveCursorToKey: Option[ProducerTransactionRecord] = {
          val keyFrom = new ProducerTransactionKey(keyStream.id, partition, from)

          iterator.seek(keyFrom.toByteArray)
          val startKey = if (iterator.isValid && comparator.compare(iterator.key(), lastTransactionID) <= 0) {
            Some(new ProducerTransactionRecord(ProducerTransactionKey.fromByteArray(iterator.key()), ProducerTransactionValue.fromByteArray(iterator.value())))
          } else None

          iterator.next()

          startKey
        }

        moveCursorToKey match {
          case None =>
            iterator.close()
            ScanTransactionsInfo(lastOpenedTransactionID, Seq())

          case Some(producerTransactionKey) =>
            val producerTransactions = ArrayBuffer[ProducerTransactionRecord](producerTransactionKey)

            //return transactions until first opened one.
            var txnState: TransactionStates = producerTransactionKey.state
            while (
              !states.contains(txnState) &&
                producerTransactions.length < count &&
                iterator.isValid &&
                (comparator.compare(iterator.key(), lastTransactionID) <= 0)
            ) {
              val producerTransaction = ProducerTransactionRecord(ProducerTransactionKey.fromByteArray(iterator.key()), ProducerTransactionValue.fromByteArray(iterator.value()))
              txnState = producerTransaction.state
              producerTransactions += producerTransaction
              iterator.next()
            }

            iterator.close()

            def producerTransactionKeyToProducerTransaction(txn: ProducerTransactionRecord) = {
              com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(keyStream.name, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.ttl)
            }

            val result = if (states.contains(txnState)) producerTransactions.init else producerTransactions
            ScanTransactionsInfo(lastOpenedTransactionID, result map producerTransactionKeyToProducerTransaction)
        }
      }
    }(executionContext.berkeleyReadContext)

  private def recordToProducerTransaction(txn: ProducerTransactionRecord, stream: String) = {
    com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(stream, txn.partition, txn.transactionID, txn.state, txn.quantity, txn.ttl)
  }

  final class TransactionsToDeleteTask(timestampToDeleteTransactions: Long) extends Callable[Unit] {
    private def doesProducerTransactionExpired(producerTransactionWithoutKey: ProducerTransactionValue): Boolean = {
      scala.math.abs(producerTransactionWithoutKey.timestamp + TimeUnit.SECONDS.toMillis(producerTransactionWithoutKey.ttl)) <= timestampToDeleteTransactions
    }

    override def call(): Unit = {
      if (logger.isDebugEnabled) logger.debug(s"Cleaner[time: $timestampToDeleteTransactions] of expired transactions is running.")
      val batch = rocksMetaServiceDB.newBatch

      def deleteTransactionIfExpired(iterator: RocksIterator): Boolean = {
        val result = if (iterator.isValid) {
          val producerTransactionValue = ProducerTransactionValue.fromByteArray(iterator.value())
          val toDelete: Boolean = doesProducerTransactionExpired(producerTransactionValue)
          if (toDelete) {
            if (logger.isDebugEnabled) logger.debug(s"Cleaning $producerTransactionValue as it's expired.")

            val producerTransactionValueTimestampUpdated = producerTransactionValue.copy(timestamp = timestampToDeleteTransactions)
            val producerTransactionKey = ProducerTransactionKey.fromByteArray(iterator.key())

            val canceledTransactionRecordDueExpiration = transitProducerTransactionToInvalidState(ProducerTransactionRecord(producerTransactionKey, producerTransactionValueTimestampUpdated))
            if (areThereAnyProducerNotifies) tryCompleteProducerNotify(ProducerTransactionRecord(producerTransactionKey, canceledTransactionRecordDueExpiration.producerTransaction))

            transactionsRamTable.invalidate(producerTransactionKey)
            batch.put(HasEnvironment.TRANSACTION_ALL_STORE, iterator.key(),  canceledTransactionRecordDueExpiration.producerTransaction.toByteArray)

            batch.remove(HasEnvironment.TRANSACTION_OPEN_STORE, iterator.key())
            true
          } else true
        } else false
        iterator.next()
        result
      }

      @tailrec
      def repeat(iterator: RocksIterator): Unit = {
        val doesExistAnyTransactionToDelete = deleteTransactionIfExpired(iterator)
        if (doesExistAnyTransactionToDelete) repeat(iterator)
        else iterator.close()
      }

      val iteratorProducerTransactionsOpened = producerTransactionsWithOpenedStateDatabase.iterator
      iteratorProducerTransactionsOpened.seekToFirst()
      repeat(iteratorProducerTransactionsOpened)
      batch.write()
    }
  }

  final def createAndExecuteTransactionsToDeleteTask(timestampToDeleteTransactions: Long): Unit = {
    executionContext.berkeleyWriteContext.submit(new TransactionsToDeleteTask(timestampToDeleteTransactions)).get()
  }
}
