package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.{Executors, ScheduledExecutorService}

import com.bwsw.commitlog.CommitLog
import com.bwsw.commitlog.filesystem.CommitLogFile
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message, MessageWithTimestamp}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import transactionService.rpc.Transaction

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContextExecutorService, Future => ScalaFuture}

class JournaledCommitLogImpl(commitLog: CommitLog, transactionServer: TransactionServer, scheduledExecutor: ScheduledExecutorService) {
  private val pathsToFilesToPutData = scala.collection.mutable.Set[String]()

  @volatile private var canBeFileProcessed = true

  private val barrier = new ResettableCountDownLatch(1)
  private def applyBarrierOnClosingCommitLogFile():Unit = {
    if (!canBeFileProcessed) barrier.countDown()
  }

  private def releaseBarrier(): Unit = {
    canBeFileProcessed = false
    commitLog.close()
    canBeFileProcessed = true
    barrier.reset
  }

  def putData(messageType: Byte, message: Message, startNew: Boolean = false) = {
    applyBarrierOnClosingCommitLogFile()
     this.synchronized {
       val messageWithTimestamp = MessageWithTimestamp(message)
       val pathToFile = commitLog.putRec(messageWithTimestamp.toByteArray, messageType, startNew)
       pathsToFilesToPutData += pathToFile
       true
     }
  }

  private val task = new Runnable {
    override def run(): Unit = {
      def readRecordsFromCommitLogFile(file: CommitLogFile) = {
        val iter = file.getIterator()
        val buffer = ArrayBuffer[(JournaledCommitLogImpl.Token, Seq[(Transaction, Long)])]()
        while (iter.hasNext()) {
          val record = iter.next()
          val (messageType, message) = record.splitAt(1)
          buffer += JournaledCommitLogImpl.retrieveTransactions(messageType.head, MessageWithTimestamp.fromByteArray(message))
        }
        buffer
      }

      releaseBarrier()


      //      def readRecordsFromCommitLogFiles(files: Seq[CommitLogFile]) = {
      //        def helper()
      //      }

      def getFileNameWithExtension(file: String) = file.substring(file.lastIndexOf("/"), file.length)

      val filesToRead = pathsToFilesToPutData.toSeq.map(path => new CommitLogFile(path)).filter(_.md5Exists())
      val records = filesToRead flatMap readRecordsFromCommitLogFile

      scala.util.Try {
        val transactionsFromValidClients = records
          .withFilter(transactionTTLWithToken => transactionServer.isValid(transactionTTLWithToken._1))
          .flatMap(x => x._2)

//        transactionServer putTransactions transactionsFromValidClients
//
//        val filesRead = filesToRead.map(_.path).toSet
//
//        pathsToFilesToPutData --= filesRead
        //
        //        val catalogue = new CommitLogCatalogue("/tmp", new java.util.Date(System.currentTimeMillis()))
        //        filesRead foreach(file => catalogue.deleteFile(getFileNameWithExtension(file)))

      } match {
        case scala.util.Success(x) => println("it's okay")
        case scala.util.Failure(error) => error.printStackTrace()
      }
    }
  }
  scheduledExecutor.scheduleWithFixedDelay(task, 0, 5, java.util.concurrent.TimeUnit.SECONDS)
}

object JournaledCommitLogImpl {
  type Token = Int
  val putTransactionType:  Byte = 0
  val putTransactionsType: Byte = 1
  val setConsumerStateType: Byte = 2

  private def deserializePutTransaction(message: Message)  = Descriptors.PutTransaction.decodeRequest(message)
  private def deserializePutTransactions(message: Message) = Descriptors.PutTransactions.decodeRequest(message)
  private def deserializeSetConsumerState(message: Message) = Descriptors.SetConsumerState.decodeRequest(message)

  private def retrieveTransactions(messageType: Byte, messageWithTimestamp: MessageWithTimestamp): (Token, Seq[(Transaction,  Long)]) = messageType match {
    case `putTransactionType`  =>
      val txn = deserializePutTransaction(messageWithTimestamp.message)
      (txn.token, Seq((txn.transaction, messageWithTimestamp.timestamp)))
    case `putTransactionsType` =>
      val txns = deserializePutTransactions(messageWithTimestamp.message)
      (txns.token, txns.transactions.map(txn => (txn, messageWithTimestamp.timestamp)))
    case `setConsumerStateType` =>
      val args = deserializeSetConsumerState(messageWithTimestamp.message)
      val consumerTransaction = transactionService.rpc.ConsumerTransaction(args.stream, args.partition, args.transaction,args.name)
      (args.token, Seq(( Transaction(None, Some(consumerTransaction)), messageWithTimestamp.timestamp)))
  }
}

