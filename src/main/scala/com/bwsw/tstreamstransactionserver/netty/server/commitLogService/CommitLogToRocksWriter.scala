
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util
import java.util.concurrent.PriorityBlockingQueue

import com.bwsw.commitlog.CommitLogRecord
import com.bwsw.commitlog.filesystem.{CommitLogBinary, CommitLogFile, CommitLogIterator, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.netty.server.commitLogReader.{BigCommitWrapper, CommitLogRecordFrame}
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.CommitLogKey
import com.bwsw.tstreamstransactionserver.netty.server.{BigCommit, RocksWriter}
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy.{Error, IncompleteCommitLogReadPolicy, SkipLog, TryRead}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import CommitLogToRocksWriter.recordsToReadNumber

private object CommitLogToRocksWriter {
  val recordsToReadNumber = 1
}

class CommitLogToRocksWriter(rocksDb: RocksDbConnection,
                             pathsToClosedCommitLogFiles: PriorityBlockingQueue[CommitLogStorage],
                             rocksWriter: => RocksWriter,
                             incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy)
  extends Runnable {

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

  private final def getRecordsReader(fileID: Long): BigCommitWrapper = {
    val value =
      CommitLogKey(fileID).toByteArray
    val bigCommit =
      new BigCommit(rocksWriter, RocksStorage.COMMIT_LOG_STORE, BigCommit.commitLogKey, value)
    new BigCommitWrapper(bigCommit)
  }

  private def readRecordsFromCommitLogFile(iter: CommitLogIterator,
                                           recordsToReadNumber: Int): (ArrayBuffer[CommitLogRecordFrame], CommitLogIterator) = {
    val buffer = ArrayBuffer.empty[CommitLogRecordFrame]
    var recordsToRead = recordsToReadNumber
    while (iter.hasNext() && recordsToRead > 0) {
      val record = iter.next()
      record.right.foreach(buffer += new CommitLogRecordFrame(_))
      recordsToRead = recordsToRead - 1
    }
    (buffer, iter)
  }

  @tailrec
  private final def processOneRecord(iterator: CommitLogIterator,
                                     commitLogReader: BigCommitWrapper,
                                     lastTransactionTimestamp: Option[Long]): Option[Long] = {

    val (commitLogRecords, iter) =
      readRecordsFromCommitLogFile(iterator, recordsToReadNumber)

    commitLogReader.addFrames(commitLogRecords)

    val isAnyElements = iter.hasNext()
    if (isAnyElements) {
      val lastTransactionTimestampInRecord =
        commitLogRecords
          .lastOption.map(_.timestamp)
          .orElse(lastTransactionTimestamp)

      processOneRecord(
        iter,
        commitLogReader,
        lastTransactionTimestampInRecord
      )
    }
    else
      lastTransactionTimestamp
  }

  @throws[Exception]
  private def processCommitLogFile(file: CommitLogStorage): Boolean = {
    val commitLogReader: BigCommitWrapper =
      getRecordsReader(file.getID)

    val iterator =
      file.getIterator
    val lastTransactionTimestamp =
      processOneRecord(iterator, commitLogReader, None)

    iterator.close()

    val result = commitLogReader.commit()

    val timestamp =
      lastTransactionTimestamp.getOrElse(System.currentTimeMillis())
    rocksWriter.createAndExecuteTransactionsToDeleteTask(timestamp)

    rocksWriter.clearProducerTransactionCache()
    result
  }

  override def run(): Unit = {
    val commitLogFiles = new util.LinkedList[CommitLogStorage]()
    pathsToClosedCommitLogFiles.drainTo(commitLogFiles)
    commitLogFiles.forEach(commitLogEntity =>
      scala.util.Try {
        processAccordingToPolicy(commitLogEntity)
      } match {
        case scala.util.Success(_) =>
        case scala.util.Failure(error) => error.printStackTrace()
      }
    )
  }

  final def closeRocksDB(): Unit = rocksDb.close()
}