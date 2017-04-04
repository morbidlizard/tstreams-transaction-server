package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.ArrayBlockingQueue

import com.bwsw.commitlog.filesystem.{CommitLogFile, CommitLogFileIterator}
import com.bwsw.tstreamstransactionserver.netty.server.{Time, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message, MessageWithTimestamp}
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy.{Error, IncompleteCommitLogReadPolicy, ResyncMajority, SkipLog, TryRead}
import com.bwsw.tstreamstransactionserver.rpc.Transaction
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class CommitLogToBerkeleyWriter(pathToRocksDB: String,
                                pathsToClosedCommitLogFiles: ArrayBlockingQueue[String],
                                transactionServer: TransactionServer,
                                incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy) extends Runnable with Time {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val processAccordingToPolicy = createProcessingFunction()

  private def createProcessingFunction() = {(path: String) =>
    incompleteCommitLogReadPolicy match {
      case ResyncMajority => (path: String) => true //todo for replicated mode use only 'resync-majority'

      case SkipLog =>
        val commitLogFile = new CommitLogFile(path)
        if (commitLogFile.md5Exists()) {
          processCommitLogFile(commitLogFile)
        } else {
          logger.warn(s"MD5 doesn't exist in a commit log file (path: '$path').")
        }

      case TryRead =>
        val commitLogFile = new CommitLogFile(path)
        try {
          processCommitLogFile(commitLogFile)
        } catch {
          case e: Exception =>
            logger.warn(s"Something was going wrong during processing of a commit log file (path: $path). Error message: " + e.getMessage)
        }

      case Error =>
        val commitLogFile = new CommitLogFile(path)
        if (commitLogFile.md5Exists()) {
          processCommitLogFile(commitLogFile)
        } else {
          logger.error(s"MD5 doesn't exist in a commit log file (path: '$path').")
          throw new InterruptedException(s"MD5 doesn't exist in a commit log file (path: '$path').")
        }
    }
  }

  private def readRecordsFromCommitLogFile(iter: CommitLogFileIterator, recordsToReadNumber: Int): (ArrayBuffer[(Transaction, Long)], CommitLogFileIterator) = {
    val buffer = ArrayBuffer[(Transaction, Long)]()
    var recordsToRead = recordsToReadNumber
    while (iter.hasNext() && recordsToRead > 0) {
      val record = iter.next()
      scala.util.Try {
        val (messageType, message) = record.splitAt(1)
        CommitLogToBerkeleyWriter.retrieveTransactions(messageType.head, MessageWithTimestamp.fromByteArray(message))
      } match {
        case scala.util.Success(transactions) => buffer ++= transactions
        case _ =>
      }
      recordsToRead = recordsToRead - 1
    }
    (buffer, iter)
  }

  @throws[Exception]
  private def processCommitLogFile(file: CommitLogFile): Boolean = {
    val recordsToReadNumber = 1
    val bigCommit = transactionServer.getBigCommit(file.getFile.getAbsolutePath)

    def getFirstRecordAndReturnIterator(iterator: CommitLogFileIterator): (CommitLogFileIterator, Seq[(Transaction, Long)]) = {
      val (records, iter) = readRecordsFromCommitLogFile(iterator, 1)
      (iter, records)
    }

    @tailrec
    def helper(iterator: CommitLogFileIterator, firstTransactionTimestamp: Long, lastTransactionTimestamp: Long): (Boolean, Long) = {
      val (records, iter) = readRecordsFromCommitLogFile(iterator, recordsToReadNumber)
      bigCommit.putSomeTransactions(records)

      val lastRecordTimestampOpt = records.lastOption match {
        case Some((transaction, timestamp)) => timestamp
        case None => lastTransactionTimestamp
      }

      val isAnyElements = iter.hasNext()
      if (isAnyElements) helper(iter, firstTransactionTimestamp, lastRecordTimestampOpt)
      else {
        iter.close()
        (bigCommit.commit(firstTransactionTimestamp), lastRecordTimestampOpt)
      }
    }

    val (iter, firstRecord) = getFirstRecordAndReturnIterator(file.getIterator)
    val isOkay = firstRecord.headOption match {
      case Some((transaction, firstTransactionTimestamp)) =>
        bigCommit.putSomeTransactions(firstRecord)
        val (areTransactionsProcessed, lastTransactionTimestamp) = helper(iter, firstTransactionTimestamp, firstTransactionTimestamp)
        if (areTransactionsProcessed) {
          if (logger.isDebugEnabled) logger.debug(s"${file.getFile.getPath} is processed successfully and all records from the file are persisted!")
          val cleanTask = transactionServer.createTransactionsToDeleteTask(lastTransactionTimestamp)
          cleanTask.run()
        } else throw new Exception("There is a bug; Stop server and fix code!")
        true

      case None =>
        iter.close()
        true
    }

    isOkay
  }

  override def run(): Unit = {
    val path = pathsToClosedCommitLogFiles.poll()
    if (path != null) {
      scala.util.Try {
        processAccordingToPolicy(path)
      } match {
        case scala.util.Success(_) =>
        case scala.util.Failure(error) => error.printStackTrace()
      }
    } else transactionServer.createTransactionsToDeleteTask(getCurrentTime).run()
  }
}

object CommitLogToBerkeleyWriter {
  val putTransactionType: Byte = 1
  val putTransactionsType: Byte = 2
  val setConsumerStateType: Byte = 3

  private def deserializePutTransaction(message: Message) = Descriptors.PutTransaction.decodeRequest(message)

  private def deserializePutTransactions(message: Message) = Descriptors.PutTransactions.decodeRequest(message)

  private def deserializeSetConsumerState(message: Message) = Descriptors.PutConsumerCheckpoint.decodeRequest(message)

  private def retrieveTransactions(messageType: Byte, messageWithTimestamp: MessageWithTimestamp): Seq[(Transaction, Long)] = messageType match {
    case `putTransactionType` =>
      val txn = deserializePutTransaction(messageWithTimestamp.message)
      Seq((txn.transaction, messageWithTimestamp.timestamp))
    case `putTransactionsType` =>
      val txns = deserializePutTransactions(messageWithTimestamp.message)
      txns.transactions.map(txn => (txn, messageWithTimestamp.timestamp))
    case `setConsumerStateType` =>
      val args = deserializeSetConsumerState(messageWithTimestamp.message)
      val consumerTransaction = com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction(args.stream, args.partition, args.transaction, args.name)
      Seq((Transaction(None, Some(consumerTransaction)), messageWithTimestamp.timestamp))
  }
}