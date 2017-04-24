package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.{Future, PriorityBlockingQueue}

import com.bwsw.commitlog.filesystem.{CommitLogBinary, CommitLogFile, CommitLogIterator, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.{Time, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message, MessageWithTimestamp}
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy.{Error, IncompleteCommitLogReadPolicy, ResyncMajority, SkipLog, TryRead}
import com.bwsw.tstreamstransactionserver.rpc.{Transaction, TransactionStates}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class CommitLogToBerkeleyWriter(rocksDb: RocksDbConnection,
                                pathsToClosedCommitLogFiles: PriorityBlockingQueue[CommitLogStorage],
                                transactionServer: TransactionServer,
                                incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy) extends Runnable with Time {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val processAccordingToPolicy = createProcessingFunction()

  private def createProcessingFunction() = { (commitLogEntity: CommitLogStorage) =>
    incompleteCommitLogReadPolicy match {
      case ResyncMajority => true //todo for replicated mode use only 'resync-majority'

      case SkipLog =>
        val fileKey = FileKey(commitLogEntity.getID)
        val fileValue = FileValue(commitLogEntity.getContent, if (commitLogEntity.md5Exists()) Some(commitLogEntity.getMD5) else None)
        rocksDb.put(fileKey.toByteArray, fileValue.toByteArray)

        commitLogEntity match {
          case file: CommitLogFile =>
            if (commitLogEntity.md5Exists()) {
              processCommitLogFile(commitLogEntity)
            } else {
              logger.warn(s"MD5 doesn't exist for the commit log file (file: '${file.getFile.getCanonicalPath}').")
            }
            file.delete()

          case binary: CommitLogBinary =>
            if (commitLogEntity.md5Exists()) {
              processCommitLogFile(commitLogEntity)
            }
            else
              logger.warn(s"MD5 doesn't exist for the commit log file (id: '${binary.getID}', retrieved from rocksdb).")
        }


      case TryRead =>
        commitLogEntity match {
          case file: CommitLogFile =>
            scala.util.Try {
              val fileKey = FileKey(commitLogEntity.getID)
              val fileValue = FileValue(commitLogEntity.getContent, if (commitLogEntity.md5Exists()) Some(commitLogEntity.getMD5) else None)
              rocksDb.put(fileKey.toByteArray, fileValue.toByteArray)
              processCommitLogFile(commitLogEntity)
            } match {
              case scala.util.Success(_) => file.delete()
              case scala.util.Failure(exception) =>
                file.delete()
                logger.warn(s"Something was going wrong during processing of a commit log file (file: ${file.getFile.getPath}). Error message: " + exception.getMessage)
            }
          case binary: CommitLogBinary =>
            scala.util.Try {
              val fileKey = FileKey(commitLogEntity.getID)
              val fileValue = FileValue(commitLogEntity.getContent, if (commitLogEntity.md5Exists()) Some(commitLogEntity.getMD5) else None)
              rocksDb.put(fileKey.toByteArray, fileValue.toByteArray)
              processCommitLogFile(commitLogEntity)
            } match {
              case scala.util.Success(_) => //do nothing
              case scala.util.Failure(exception) =>
                logger.warn(s"Something was going wrong during processing of a commit log file (id: ${binary.getID}, retrieved from rocksdb). Error message: " + exception.getMessage)
            }
        }

      case Error =>
        val fileKey = FileKey(commitLogEntity.getID)
        val fileValue = FileValue(commitLogEntity.getContent, if (commitLogEntity.md5Exists()) Some(commitLogEntity.getMD5) else None)
        rocksDb.put(fileKey.toByteArray, fileValue.toByteArray)

        if (commitLogEntity.md5Exists()) {
          processCommitLogFile(commitLogEntity)
        } else commitLogEntity match {
          case file: CommitLogFile =>
            file.delete()
            throw new InterruptedException(s"MD5 doesn't exist in a commit log file (path: '${file.getFile.getPath}').")
          case binary: CommitLogBinary =>
            throw new InterruptedException(s"MD5 doesn't exist in a commit log file (id: '${binary.getID}').")
        }
    }
  }

  private def readRecordsFromCommitLogFile(iter: CommitLogIterator, recordsToReadNumber: Int): (ArrayBuffer[(Transaction, Long)], CommitLogIterator) = {
    val buffer = ArrayBuffer[(Transaction, Long)]()
    var recordsToRead = recordsToReadNumber
    while (iter.hasNext() && recordsToRead > 0) {
      val record = iter.next()
      record.right.foreach { record =>
        scala.util.Try {
          val (messageType, message) = (record.messageType, record.message)
          val messageWithTimestamp = MessageWithTimestamp.fromByteArray(message)
          CommitLogToBerkeleyWriter.retrieveTransactions(messageType, messageWithTimestamp)
        } match {
          case scala.util.Success(transactions) => buffer ++= transactions
          case _ =>
        }
      }
      recordsToRead = recordsToRead - 1
    }
    (buffer, iter)
  }

  @throws[Exception]
  private def processCommitLogFile(file: CommitLogStorage): Boolean = {
    val recordsToReadNumber = 1
    val bigCommit = transactionServer.getBigCommit(file.getID)

    @tailrec
    def helper(iterator: CommitLogIterator): Unit = {
      val (records, iter) = readRecordsFromCommitLogFile(iterator, recordsToReadNumber)
      bigCommit.putSomeTransactions(records)
      val isAnyElements = iter.hasNext()
      if (isAnyElements)
        helper(iter)
      else
        ()
    }

    val iterator = file.getIterator
    helper(iterator)
    iterator.close()
    val result = bigCommit.commit()
    result
  }

  override def run(): Unit = {
    while(pathsToClosedCommitLogFiles.size() > 0) {
      val commitLogEntity = pathsToClosedCommitLogFiles.poll()

      if (commitLogEntity != null) {
        scala.util.Try {
          processAccordingToPolicy(commitLogEntity)
        } match {
          case scala.util.Success(_) =>
          case scala.util.Failure(error) => error.printStackTrace()
        }
      }
    }
    if (pathsToClosedCommitLogFiles.isEmpty) transactionServer.createAndExecuteTransactionsToDeleteTask(getCurrentTime)
  }

  final def closeRocksDB(): Unit = rocksDb.close()
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