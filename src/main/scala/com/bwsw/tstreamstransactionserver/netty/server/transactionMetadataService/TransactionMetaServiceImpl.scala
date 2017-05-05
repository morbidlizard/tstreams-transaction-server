package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceImpl, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{Batch, RocksDBALL}
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCache, StreamKey}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.{KeyStreamPartition, LastTransactionStreamPartition, TransactionStateHandler}
import com.bwsw.tstreamstransactionserver.netty.server.RocksStorage
import com.bwsw.tstreamstransactionserver.rpc._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class TransactionMetaServiceImpl(rocksMetaServiceDB: RocksDBALL,
                                 streamCache: StreamCache,
                                 lastTransactionStreamPartition: LastTransactionStreamPartition,
                                 consumerService: ConsumerServiceImpl)
  extends TransactionStateHandler
    with ProducerTransactionStateNotifier
{
  import lastTransactionStreamPartition._
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val producerTransactionsDatabase = rocksMetaServiceDB.getDatabase(RocksStorage.TRANSACTION_ALL_STORE)
  private val producerTransactionsWithOpenedStateDatabase = rocksMetaServiceDB.getDatabase(RocksStorage.TRANSACTION_OPEN_STORE)

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
      Option(producerTransactionsWithOpenedStateDatabase.get(keyFound)).map { data =>
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
          val key = KeyStreamPartition(txn.stream, txn.partition)
          if (txn.state != TransactionStates.Opened) {
            producerTransactions += ((txn, timestamp))
          } else if (!isThatTransactionOutOfOrder(key, txn.transactionID)) {
            // updating RAM table, and last opened transaction database.
            updateLastTransactionStreamPartitionRamTable(key, txn.transactionID, isOpenedTransaction = true)
            putLastTransaction(key, txn.transactionID, isOpenedTransaction = true, batch)
            if (logger.isDebugEnabled) logger.debug(s"On stream:${key.stream} partition:${key.partition} last opened transaction is ${txn.transactionID} now.")
            producerTransactions += ((txn, timestamp))
          }

        case (_, Some(txn)) =>
          consumerTransactions += ((txn, timestamp))

        case _ =>
      }
    }
    (producerTransactions, consumerTransactions)
  }

  private final def groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(txns: Seq[(ProducerTransaction, Timestamp)]): Map[StreamKey, ArrayBuffer[ProducerTransactionRecord]] = {
    if (logger.isDebugEnabled) logger.debug("Mapping all producer transactions streams attrbute to long representation(ID), grouping them by stream and partition, checking that the stream isn't deleted in order to process producer transactions.")
    txns.foldLeft[scala.collection.mutable.Map[StreamKey, ArrayBuffer[ProducerTransactionRecord]]](scala.collection.mutable.Map()) { case (acc, (producerTransaction, timestamp)) =>
      val keyStream = StreamKey(producerTransaction.stream)
      if (acc.contains(keyStream))
        acc(keyStream) += ProducerTransactionRecord(producerTransaction, timestamp)
      else
        acc += ((keyStream, ArrayBuffer(ProducerTransactionRecord(producerTransaction, timestamp))))
      acc
    }.toMap
  }


  private final def decomposeConsumerTransactionsToDatabaseRepresentation(transactions: Seq[(ConsumerTransaction, Timestamp)]) = {
    val consumerTransactionsKey = ArrayBuffer[ConsumerTransactionRecord]()
    transactions.foreach { case (txn, timestamp) =>
      consumerTransactionsKey += ConsumerTransactionRecord(txn, timestamp)
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

  private def putTransactions(transactions: Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Long)], batch: Batch): ListBuffer[Unit => Unit] = {
    val (producerTransactions, consumerTransactions) = decomposeTransactionsToProducerTxnsAndConsumerTxns(transactions, batch)
    val groupedProducerTransactionsWithTimestamp = groupProducerTransactionsByStreamAndDecomposeThemToDatabaseRepresentation(producerTransactions)

    val notifications = new scala.collection.mutable.ListBuffer[Unit => Unit]()
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
              batch.put(RocksStorage.TRANSACTION_OPEN_STORE, binaryKey, binaryTxn)
            }
            else {
              batch.remove(RocksStorage.TRANSACTION_OPEN_STORE, binaryKey)
            }

            if (areThereAnyProducerNotifies) notifications += tryCompleteProducerNotify(producerTransactionRecord)

            batch.put(RocksStorage.TRANSACTION_ALL_STORE, binaryKey, binaryTxn)
            if (logger.isDebugEnabled) logger.debug(s"Producer transaction on stream: ${producerTransactionRecord.stream}" +
              s"partition ${producerTransactionRecord.partition}, transactionId ${producerTransactionRecord.transactionID} " +
              s"with state ${producerTransactionRecord.state} is ready for commit[commit id: ${batch.id}]"
            )
          case scala.util.Failure(throwable) => //throwable.printStackTrace()
        }
      }
    }
    val consumerTransactionsToProcess = decomposeConsumerTransactionsToDatabaseRepresentation(consumerTransactions)
    val notificationAboutProducerAndConsumerTransactiomns = if (consumerTransactionsToProcess.nonEmpty)
      notifications ++ consumerService.putConsumersCheckpoints(consumerTransactionsToProcess, batch)
    else
      notifications
    notificationAboutProducerAndConsumerTransactiomns
  }


  private val commitLogDatabase = rocksMetaServiceDB.getDatabase(RocksStorage.COMMIT_LOG_STORE)

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
    private val notifications = new scala.collection.mutable.ListBuffer[Unit => Unit]

    def putSomeTransactions(transactions: Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Long)]): Unit = {
      if (logger.isDebugEnabled) logger.debug("Adding to commit new transactions from commit log file.")
      notifications ++= putTransactions(transactions, batch)
    }

    def commit(): Boolean = {
      val key = CommitLogKey(fileID).toByteArray
      val value = Array[Byte]()

      batch.put(RocksStorage.COMMIT_LOG_STORE, key, value)
      if (batch.write()) {
        if (logger.isDebugEnabled) logger.debug(s"commit ${batch.id} is successfully fixed.")
        notifications foreach (notification => notification(()))
        true
      } else {
        false
      }
    }
  }

  def getBigCommit(fileID: Long) = new BigCommit(fileID)


  final def getTransaction(streamID: Int, partition: Int, transaction: Long): com.bwsw.tstreamstransactionserver.rpc.TransactionInfo = {
    val lastTransaction = getLastTransactionIDAndCheckpointedID(streamID, partition)
    if (lastTransaction.isEmpty || transaction > lastTransaction.get.opened.id) {
      TransactionInfo(exists = false, None)
    } else {
      val searchKey = new ProducerTransactionKey(streamID, partition, transaction).toByteArray

      Option(producerTransactionsDatabase.get(searchKey)).map(searchData =>
        new ProducerTransactionRecord(ProducerTransactionKey.fromByteArray(searchKey), ProducerTransactionValue.fromByteArray(searchData))
      ) match {
        case None =>
          TransactionInfo(exists = true, None)
        case Some(producerTransactionRecord) =>
          TransactionInfo(exists = true, Some(producerTransactionRecord))
      }
    }
  }

  final def getLastCheckpointedTransaction(streamID: Int, partition: Int): Option[Long] = {
    val result = getLastTransactionIDAndCheckpointedID(streamID, partition) match {
      case Some(last) => last.checkpointed match {
        case Some(checkpointed) => Some(checkpointed.id)
        case None => None
      }
      case None => None
    }
    result
  }

  private val comparator = com.bwsw.tstreamstransactionserver.`implicit`.Implicits.ByteArray

  def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: collection.Set[TransactionStates]): com.bwsw.tstreamstransactionserver.rpc.ScanTransactionsInfo =
    {
      val (lastOpenedTransactionID, toTransactionID) = getLastTransactionIDAndCheckpointedID(streamID, partition) match {
        case Some(lastTransaction) => lastTransaction.opened.id match {
          case lt if lt < from => (lt, from - 1L)
          case lt if from <= lt && lt < to => (lt, lt)
          case lt if lt >= to => (lt, to)
        }
        case None => (-1L, from - 1L)
      }

      if (logger.isDebugEnabled) logger.debug(s"Trying to retrieve transactions on stream $streamID, partition: $partition in range [$from, $to]." +
        s"Actually as lt ${if (lastOpenedTransactionID == -1) "doesn't exist" else s"is $lastOpenedTransactionID"} the range is [$from, $toTransactionID].")

      if (toTransactionID < from || count == 0) ScanTransactionsInfo(lastOpenedTransactionID, Seq())
      else {
        val iterator = producerTransactionsDatabase.iterator

        val lastTransactionID = new ProducerTransactionKey(streamID, partition, toTransactionID).toByteArray
        def moveCursorToKey: Option[ProducerTransactionRecord] = {
          val keyFrom = new ProducerTransactionKey(streamID, partition, from)

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

            val result = if (states.contains(txnState)) producerTransactions.init else producerTransactions
            ScanTransactionsInfo(lastOpenedTransactionID, result)
        }
      }
    }


  def transactionsToDeleteTask(timestampToDeleteTransactions: Long) {
    def doesProducerTransactionExpired(producerTransactionWithoutKey: ProducerTransactionValue): Boolean = {
      scala.math.abs(producerTransactionWithoutKey.timestamp + TimeUnit.SECONDS.toMillis(producerTransactionWithoutKey.ttl)) <= timestampToDeleteTransactions
    }

    if (logger.isDebugEnabled) logger.debug(s"Cleaner[time: $timestampToDeleteTransactions] of expired transactions is running.")
    val batch = rocksMetaServiceDB.newBatch

    val iterator = producerTransactionsWithOpenedStateDatabase.iterator
    iterator.seekToFirst()

    val notifications = new ListBuffer[Unit => Unit]()
    while (iterator.isValid) {
      val producerTransactionValue = ProducerTransactionValue.fromByteArray(iterator.value())
      if (doesProducerTransactionExpired(producerTransactionValue)) {
        if (logger.isDebugEnabled) logger.debug(s"Cleaning $producerTransactionValue as it's expired.")

        val producerTransactionValueTimestampUpdated = producerTransactionValue.copy(timestamp = timestampToDeleteTransactions)
        val key = iterator.key()
        val producerTransactionKey = ProducerTransactionKey.fromByteArray(key)

        val canceledTransactionRecordDueExpiration = transitProducerTransactionToInvalidState(ProducerTransactionRecord(producerTransactionKey, producerTransactionValueTimestampUpdated))
        if (areThereAnyProducerNotifies)
          notifications += tryCompleteProducerNotify(ProducerTransactionRecord(producerTransactionKey, canceledTransactionRecordDueExpiration.producerTransaction))

        transactionsRamTable.invalidate(producerTransactionKey)
        batch.put(RocksStorage.TRANSACTION_ALL_STORE, key, canceledTransactionRecordDueExpiration.producerTransaction.toByteArray)

        batch.remove(RocksStorage.TRANSACTION_OPEN_STORE, key)
      }
      iterator.next()
    }
    iterator.close()
    batch.write()

    notifications.foreach(notification => notification(()))
  }

  final def createAndExecuteTransactionsToDeleteTask(timestampToDeleteTransactions: Long): Unit = {
    transactionsToDeleteTask(timestampToDeleteTransactions)
  }
}
