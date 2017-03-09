package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.Executors

import com.bwsw.commitlog.CommitLog
import com.bwsw.commitlog.CommitLogFlushPolicy.{OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TimestampCommitLog
import com.bwsw.tstreamstransactionserver.netty.{Message, MessageWithTimestamp}
import com.bwsw.tstreamstransactionserver.options.CommitLogWriteSyncPolicy.{EveryNSeconds, EveryNewFile, EveryNth}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.CommitLogOptions
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.sleepycat.je._

class JournaledCommitLogImpl(transactionServer: TransactionServer,
                             commitLogOptions: CommitLogOptions) {
  private val scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("CommitLog-%d").build())
  private val commitLog = createCommitLog()
  @volatile private var isFileProcessable = true
  private val barrier: ResettableCountDownLatch = new ResettableCountDownLatch(1)

  private def createCommitLog() = {
    val policy = commitLogOptions.commitLogWriteSyncPolicy match {
      case EveryNth => OnCountInterval(commitLogOptions.commitLogWriteSyncValue)
      case EveryNewFile => OnRotation
      case EveryNSeconds => OnTimeInterval(commitLogOptions.commitLogWriteSyncValue)
    }

    new CommitLog(Int.MaxValue, "/tmp", policy)
  }

  private def applyBarrierOnClosingCommitLogFile(): Unit = {
    if (!isFileProcessable) barrier.countDown()
  }

  def putData(messageType: Byte, message: Message, startNew: Boolean = false) = {
    applyBarrierOnClosingCommitLogFile()
    this.synchronized(commitLog.putRec(MessageWithTimestamp(message).toByteArray, messageType, startNew))
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

    val keyFound = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val processedCommitLogFiles = scala.collection.mutable.ArrayBuffer[String]()

    val cursor = commitLogDatabase.openCursor(null, null)

    while (cursor.getNext(keyFound, dataFound, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
      processedCommitLogFiles += TimestampCommitLog.pathToObject(dataFound)
    }
    cursor.close()

    commitLogDatabase.close()
    transactionMetaEnvironment.close()

    processedCommitLogFiles
  }

  private val catalogue = new CommitLogCatalogue("/tmp", new java.util.Date(System.currentTimeMillis()))
  //TODO if there in no directory exist before method is called exception will be thrown
  //  catalogue.listAllFiles() foreach (x => println(x.getFile().getAbsolutePath))
  //  println(getProcessedCommitLogFiles)

  private val task = new CommitLogToBerkeleyWriter(commitLog, transactionServer, barrier, isFileProcessable, commitLogOptions.incompleteCommitLogReadPolicy)

  scheduledExecutor.scheduleWithFixedDelay(task, 0, 2, java.util.concurrent.TimeUnit.SECONDS)

  def shutdown() = {
    scheduledExecutor.shutdown()
  }
}