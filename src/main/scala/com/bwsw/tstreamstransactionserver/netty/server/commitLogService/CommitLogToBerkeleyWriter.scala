package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import com.bwsw.commitlog.CommitLog
import com.bwsw.commitlog.filesystem.{CommitLogFile, CommitLogFileIterator}
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.CommitLogToBerkeleyWriter.Token
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message, MessageWithTimestamp}
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy.{Error, IncompleteCommitLogReadPolicy, ResyncMajority, SkipLog, TryRead}
import org.slf4j.LoggerFactory
import transactionService.rpc.Transaction

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class CommitLogToBerkeleyWriter(commitLog: CommitLog,
                                transactionServer: TransactionServer,
                                barrier: ResettableCountDownLatch,
                                @volatile var isFileProcessable: Boolean,
                                incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy) extends Runnable {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var pathsToFilesToPutData = Array[String]()
  private val recordsToReadNumber = 1000000
  private val processAccordingToPolicy = createProcessingFunction()

  private def createProcessingFunction() = {
    incompleteCommitLogReadPolicy match {
      case ResyncMajority => (path: String) => true //todo for replicated mode use only 'resync-majority'

      case SkipLog => (path: String) => {
        val commitLogFile = new CommitLogFile(path)
        if (commitLogFile.md5Exists()) {
          processCommitLogFile(commitLogFile, recordsToReadNumber)
        } else {
          logger.warn(s"MD5 doesn't exist in a commit log file (path: '$path').")

          true
        }
      }

      case TryRead => (path: String) => {
        try {
          val commitLogFile = new CommitLogFile(path)
          processCommitLogFile(commitLogFile, recordsToReadNumber)
        } catch {
          case e: Exception =>
            logger.warn(s"Something was going wrong during processing of a commit log file (path: $path). Error message: " + e.getMessage)
        }
      }

      case Error => (path: String) => {
        val commitLogFile = new CommitLogFile(path)
        if (commitLogFile.md5Exists()) {
          processCommitLogFile(commitLogFile, recordsToReadNumber)
        } else {
          logger.error(s"MD5 doesn't exist in a commit log file (path: '$path').")
          throw new InterruptedException(s"MD5 doesn't exist in a commit log file (path: '$path').")
        }
      }
    }
  }

  private def readRecordsFromCommitLogFile(iter: CommitLogFileIterator, recordsToReadNumber: Int): (ArrayBuffer[(Token, Seq[(Transaction, Long)])], CommitLogFileIterator) = {
    val buffer = ArrayBuffer[(CommitLogToBerkeleyWriter.Token, Seq[(Transaction, Long)])]()
    var recordsToRead = recordsToReadNumber
    while (iter.hasNext() && recordsToRead > 0) {
      val record = iter.next()
      val (messageType, message) = record.splitAt(1)
      buffer += CommitLogToBerkeleyWriter.retrieveTransactions(messageType.head, MessageWithTimestamp.fromByteArray(message))
      recordsToRead = recordsToRead - 1
    }
    (buffer, iter)
  }

  @throws[Exception]
  private def processCommitLogFile(file: CommitLogFile, recordsToReadNumber: Int): Boolean = {
    lazy val bigCommit = transactionServer.getBigCommit(file.attributes.creationTime.toMillis, file.getFile().getAbsolutePath)

    @tailrec
    def helper(iterator: CommitLogFileIterator): Boolean = {
      val (records, iter) = readRecordsFromCommitLogFile(iterator, recordsToReadNumber)
      val transactionsFromValidClients = records
        .withFilter(transactionTTLWithToken => transactionServer.isValid(transactionTTLWithToken._1))
        .flatMap(x => x._2)

      bigCommit.putSomeTransactions(transactionsFromValidClients)

      val isAnyElements = scala.util.Try(iter.hasNext()).getOrElse(false)

      if (isAnyElements) helper(iter) else bigCommit.commit()
      // bigCommit.commit() else bigCommit.abort()
    }

    val isOkay = helper(file.getIterator())

    if (isOkay) {
      val cleanTask = transactionServer.createTransactionsToDeleteTask(file.attributes.lastModifiedTime.toMillis)
      cleanTask.run()
      pathsToFilesToPutData = pathsToFilesToPutData.tail
    } else throw new Exception("There is a bug; Stop server and fix code!")

    isOkay
  }

  override def run(): Unit = {
    releaseBarrier()

    pathsToFilesToPutData.foreach(path =>
      scala.util.Try {
        processAccordingToPolicy(path)
      } match {
        case scala.util.Success(_) => println("it's okay")
        case scala.util.Failure(error) => error.printStackTrace()
      })
  }

  private def releaseBarrier(): Unit = {
    isFileProcessable = false
    pathsToFilesToPutData = pathsToFilesToPutData.+:(commitLog.close())
    isFileProcessable = true
    barrier.reset
  }
}

object CommitLogToBerkeleyWriter {
  type Token = Int
  val putTransactionType: Byte = 1
  val putTransactionsType: Byte = 2
  val setConsumerStateType: Byte = 3

  private def deserializePutTransaction(message: Message) = Descriptors.PutTransaction.decodeRequest(message)

  private def deserializePutTransactions(message: Message) = Descriptors.PutTransactions.decodeRequest(message)

  private def deserializeSetConsumerState(message: Message) = Descriptors.SetConsumerState.decodeRequest(message)

  private def retrieveTransactions(messageType: Byte, messageWithTimestamp: MessageWithTimestamp): (Token, Seq[(Transaction, Long)]) = messageType match {
    case `putTransactionType` =>
      val txn = deserializePutTransaction(messageWithTimestamp.message)
      (txn.token, Seq((txn.transaction, messageWithTimestamp.timestamp)))
    case `putTransactionsType` =>
      val txns = deserializePutTransactions(messageWithTimestamp.message)
      (txns.token, txns.transactions.map(txn => (txn, messageWithTimestamp.timestamp)))
    case `setConsumerStateType` =>
      val args = deserializeSetConsumerState(messageWithTimestamp.message)
      val consumerTransaction = transactionService.rpc.ConsumerTransaction(args.stream, args.partition, args.transaction, args.name)
      (args.token, Seq((Transaction(None, Some(consumerTransaction)), messageWithTimestamp.timestamp)))
  }
}