package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.PriorityBlockingQueue

import com.bwsw.commitlog.CommitLogRecord
import com.bwsw.commitlog.filesystem.{CommitLogBinary, CommitLogFile, CommitLogIterator, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.{Time, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy.{Error, IncompleteCommitLogReadPolicy, SkipLog, TryRead}
import com.bwsw.tstreamstransactionserver.rpc.Transaction
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class CommitLogToBerkeleyWriter(rocksDb: RocksDbConnection,
                                pathsToClosedCommitLogFiles: PriorityBlockingQueue[CommitLogStorage],
                                transactionServer: TransactionServer,
                                incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy)
  extends Runnable
    with Time
{
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val processAccordingToPolicy = createProcessingFunction()

  private def createProcessingFunction() = { (commitLogEntity: CommitLogStorage) =>
    incompleteCommitLogReadPolicy match {
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
          CommitLogToBerkeleyWriter.retrieveTransactions(record)
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
    def helper(iterator: CommitLogIterator, lastTransationTimestamp: Option[Long]): Option[Long] = {
      val (records, iter) = readRecordsFromCommitLogFile(iterator, recordsToReadNumber)
      bigCommit.putSomeTransactions(records)
      val isAnyElements = iter.hasNext()
      if (isAnyElements) {
        val lastTransactionTimestampInRecord = records.lastOption.map(_._2)
        helper(iter, if (lastTransactionTimestampInRecord.isDefined) lastTransactionTimestampInRecord else lastTransationTimestamp)
      }
      else
        lastTransationTimestamp
    }

    val iterator = file.getIterator
    val lastTransactionTimestamp = helper(iterator, None)
    iterator.close()
    val result = bigCommit.commit()
    lastTransactionTimestamp.orElse(Some(System.currentTimeMillis()))
      .foreach (timestamp => transactionServer.createAndExecuteTransactionsToDeleteTask(timestamp))
    result
  }

  override def run(): Unit = {
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

  final def closeRocksDB(): Unit = rocksDb.close()
}

object CommitLogToBerkeleyWriter {
  val putTransactionType: Byte = 1
  val putTransactionsType: Byte = 2
  val setConsumerStateType: Byte = 3

  private def deserializePutTransaction(message: Array[Byte]) = Protocol.PutTransaction.decodeRequest(message)

  private def deserializePutTransactions(message: Array[Byte]) = Protocol.PutTransactions.decodeRequest(message)

  private def deserializeSetConsumerState(message: Array[Byte]) = Protocol.PutConsumerCheckpoint.decodeRequest(message)


  def retrieveTransactions(record: CommitLogRecord): Seq[(Transaction, Long)] = record.messageType match {
    case `putTransactionType` =>
      val txn = deserializePutTransaction(record.message)
      Seq((txn.transaction, record.timestamp))
    case `putTransactionsType` =>
      val txns = deserializePutTransactions(record.message)
      txns.transactions.map(txn => (txn, record.timestamp))
    case `setConsumerStateType` =>
      val args = deserializeSetConsumerState(record.message)
      val consumerTransaction = com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction(args.streamID, args.partition, args.transaction, args.name)
      Seq((Transaction(None, Some(consumerTransaction)), record.timestamp))
    case _ => throw new IllegalArgumentException("Undefined method type for retrieving message from commit log record")
  }
}