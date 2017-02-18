package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService}

import com.bwsw.commitlog.CommitLog
import com.bwsw.commitlog.filesystem.{CommitLogCatalogue, CommitLogFile, CommitLogFileIterator}
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.JournaledCommitLogImpl.Token
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetaService.TimestampCommitLog
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message, MessageWithTimestamp}
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import com.sleepycat.je._
import transactionService.rpc.Transaction

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContextExecutorService, Future => ScalaFuture}

class JournaledCommitLogImpl(commitLog: CommitLog, transactionServer: TransactionServer, scheduledExecutor: ScheduledExecutorService) {
  private val pathsToFilesToPutData = ConcurrentHashMap.newKeySet[String]()
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
    val pathToFile = this.synchronized(
      commitLog.putRec(MessageWithTimestamp(message).toByteArray, messageType, startNew)
    )
    pathsToFilesToPutData.add(pathToFile)
    true
  }

  def getProcessedCommitLogFiles = {
    val directory = FileUtils.createDirectory(transactionServer.storageOpts.metadataDirectory, transactionServer.storageOpts.path)
    val defaultDurability = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE)
    val transactionMetaEnvironment = {
      val environmentConfig = new EnvironmentConfig()
        .setAllowCreate(true)
        .setTransactional(true)
        .setSharedCache(true)
      //    config.berkeleyDBJEproperties foreach {
      //      case (name, value) => environmentConfig.setConfigParam(name,value)
      //    } //todo it will be deprecated soon

      environmentConfig.setDurabilityVoid(defaultDurability)
      new Environment(directory, environmentConfig)
    }

    val commitLogDatabase = {
      val dbConfig = new DatabaseConfig()
        .setAllowCreate(true)
        .setTransactional(true)
      val storeName = "CommitLogStore"
      transactionMetaEnvironment.openDatabase(null, storeName, dbConfig)
    }

    val keyFound  = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val processedCommitLogFiles = scala.collection.mutable.ArrayBuffer[String]()

    val cursor = commitLogDatabase.openCursor(new DiskOrderedCursorConfig())

    while (cursor.getNext(keyFound, dataFound, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
      processedCommitLogFiles += TimestampCommitLog.pathToObject(dataFound)
    }
    cursor.close()

    commitLogDatabase.close()
    transactionMetaEnvironment.close()

    processedCommitLogFiles
  }

  val catalogue = new CommitLogCatalogue("/tmp", new java.util.Date(System.currentTimeMillis()))
  //TODO if there in no directory exist before method is called exception will be thrown
//  catalogue.listAllFiles() foreach (x => println(x.getFile().getAbsolutePath))
//  println()


  private val task = new Runnable {
    override def run(): Unit = {
      def readRecordsFromCommitLogFile(iter: CommitLogFileIterator, recordsToReadNumber: Int): (ArrayBuffer[(Token, Seq[(Transaction, Long)])], CommitLogFileIterator) = {
        val buffer = ArrayBuffer[(JournaledCommitLogImpl.Token, Seq[(Transaction, Long)])]()
        var recordsToRead = recordsToReadNumber
        while (iter.hasNext() && recordsToRead > 0) {
          val record = iter.next()
          val (messageType, message) = record.splitAt(1)
          buffer += JournaledCommitLogImpl.retrieveTransactions(messageType.head, MessageWithTimestamp.fromByteArray(message))
          recordsToRead = recordsToRead - 1
        }
        (buffer, iter)
      }


      def isProcessedSuccessfullyCommitLogFile(file: CommitLogFile, recordsToReadNumber: Int): Boolean = {
        val bigCommit = transactionServer.getBigCommit(file.attributes.creationTime.toMillis, file.getFile().getAbsolutePath)
        @tailrec
        def helper(iterator: CommitLogFileIterator): Boolean = {
          val (records, iter) = readRecordsFromCommitLogFile(iterator, recordsToReadNumber)
          val transactionsFromValidClients = records
            .withFilter(transactionTTLWithToken => transactionServer.isValid(transactionTTLWithToken._1))
            .flatMap(x => x._2)

          val okay = bigCommit.putSomeTransactions(transactionsFromValidClients)

          val isAnyElements = scala.util.Try(iter.hasNext()).getOrElse(false)
          if (okay && isAnyElements) helper(iter)
          else if (okay) bigCommit.commit() else bigCommit.abort()
        }
        helper(file.getIterator())
      }

      @tailrec @throws[Exception]
      def processCommitLogFiles(commitLogFiles: List[CommitLogFile], recordsToReadNumber: Int): Unit = commitLogFiles match {
        case Nil => ()
        case head :: Nil =>
          if (isProcessedSuccessfullyCommitLogFile(head, recordsToReadNumber)) {
            pathsToFilesToPutData.remove(head.getFile().getAbsolutePath)
          }
          else
            throw new Exception("There is a bug; Stop server and fix code!")

        case head :: tail =>
          if (isProcessedSuccessfullyCommitLogFile(head, recordsToReadNumber)) {
            pathsToFilesToPutData.remove(head.getFile().getAbsolutePath)
            processCommitLogFiles(tail, recordsToReadNumber)
          }
          else
            throw new Exception("There is a bug; Stop server and fix code!")
      }


      releaseBarrier()
      scala.util.Try {
        import scala.collection.JavaConverters._
        val filesToRead = pathsToFilesToPutData.asScala.map(path => new CommitLogFile(path)).filter(_.md5Exists())
        processCommitLogFiles(filesToRead.toList, 1000000)
      } match {
        case scala.util.Success(x) => println("it's okay")
        case scala.util.Failure(error) => error.printStackTrace()
      }



//      scala.util.Try {
//        val transactionsFromValidClients = records
//          .withFilter(transactionTTLWithToken => transactionServer.isValid(transactionTTLWithToken._1))
//          .flatMap(x => x._2)
//
//        transactionServer.getBigCommit
//        transactionServer putTransactions transactionsFromValidClients
//
//        val filesRead = filesToRead.map(_.path).toSet
//
//        pathsToFilesToPutData --= filesRead
        //
        //        val catalogue = new CommitLogCatalogue("/tmp", new java.util.Date(System.currentTimeMillis()))
        //        filesRead foreach(file => catalogue.deleteFile(getFileNameWithExtension(file)))

//      } match {
//        case scala.util.Success(x) => println("it's okay")
//        case scala.util.Failure(error) => error.printStackTrace()
//      }
    }
  }
  scheduledExecutor.scheduleWithFixedDelay(task, 0, 10, java.util.concurrent.TimeUnit.SECONDS)
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

