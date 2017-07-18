
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

import java.util.concurrent.PriorityBlockingQueue

import com.bwsw.commitlog.CommitLogRecord
import com.bwsw.commitlog.filesystem.{CommitLogBinary, CommitLogFile, CommitLogIterator, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.{RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy.{Error, IncompleteCommitLogReadPolicy, SkipLog, TryRead}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class CommitLogToRocksWriter(rocksDb: RocksDbConnection,
                             pathsToClosedCommitLogFiles: PriorityBlockingQueue[CommitLogStorage],
                             transactionServer: TransactionServer,
                             incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy)
  extends Runnable
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

  private def readRecordsFromCommitLogFile(iter: CommitLogIterator,
                                           recordsToReadNumber: Int): (ArrayBuffer[CommitLogRecord], CommitLogIterator) = {
    val buffer = ArrayBuffer[CommitLogRecord]()
    var recordsToRead = recordsToReadNumber
    while (iter.hasNext() && recordsToRead > 0) {
      val record = iter.next()
      record.right.map(buffer += _)
      recordsToRead = recordsToRead - 1
    }
    (buffer, iter)
  }

  @throws[Exception]
  private def processCommitLogFile(file: CommitLogStorage): Boolean = {
    val recordsToReadNumber = 1
    val bigCommit = transactionServer.getBigCommit(file.getID)

    @tailrec
    def helper(iterator: CommitLogIterator, lastTransactionTimestamp: Option[Long]): Option[Long] = {
      val (commitLogRecords, iter) = readRecordsFromCommitLogFile(iterator, recordsToReadNumber)

      val producerTransactionsRecords = new ArrayBuffer[ProducerTransactionRecord]()
      val consumerTransactionsRecords = new ArrayBuffer[ConsumerTransactionRecord]()
      commitLogRecords.foreach { record =>
        RecordType(record.messageType) match {
          case RecordType.PutConsumerCheckpointType =>
            val consumerTransactionAsStruct =
              RecordType.deserializePutConsumerCheckpoint(record.message)

            consumerTransactionsRecords += {
              import consumerTransactionAsStruct._
              ConsumerTransactionRecord(name,
                streamID,
                partition,
                transaction,
                record.timestamp
              )
            }

          case RecordType.PutTransactionType =>
            val transactionAsStruct =
              RecordType.deserializePutTransaction(record.message)
                .transaction

            transactionAsStruct.producerTransaction.foreach(producerTransaction =>
              producerTransactionsRecords += ProducerTransactionRecord(producerTransaction, record.timestamp)
            )

            transactionAsStruct.consumerTransaction.foreach(consumerTransaction =>
              consumerTransactionsRecords += ConsumerTransactionRecord(consumerTransaction, record.timestamp)
            )
          case RecordType.PutTransactionsType =>
            val transactionsAsStructs =
              RecordType.deserializePutTransactions(record.message)
                .transactions

            transactionsAsStructs.foreach { transactionAsStruct =>
              transactionAsStruct.producerTransaction.foreach(producerTransaction =>
                producerTransactionsRecords += ProducerTransactionRecord(producerTransaction, record.timestamp)
              )

              transactionAsStruct.consumerTransaction.foreach(consumerTransaction =>
                consumerTransactionsRecords += ConsumerTransactionRecord(consumerTransaction, record.timestamp)
              )
            }
          case _ =>
        }
      }

      bigCommit.putProducerTransactions(producerTransactionsRecords)
      bigCommit.putConsumerTransactions(consumerTransactionsRecords)

      val isAnyElements = iter.hasNext()
      if (isAnyElements) {
        val lastTransactionTimestampInRecord =
          commitLogRecords.lastOption.map(_.timestamp)

        helper(
          iter,
          lastTransactionTimestampInRecord.
            orElse(lastTransactionTimestamp)
        )
      }
      else
        lastTransactionTimestamp
    }

    val iterator = file.getIterator
    val lastTransactionTimestamp = helper(iterator, None)
    iterator.close()

    val result = bigCommit.commit()

    val timestamp = lastTransactionTimestamp.getOrElse(System.currentTimeMillis())
    transactionServer.createAndExecuteTransactionsToDeleteTask(timestamp)

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