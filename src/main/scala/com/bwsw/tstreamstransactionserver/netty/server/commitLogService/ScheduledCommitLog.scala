package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicReference

import com.bwsw.commitlog.CommitLog
import com.bwsw.commitlog.CommitLogFlushPolicy.{OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.tstreamstransactionserver.netty.server.Time
import com.bwsw.tstreamstransactionserver.netty.{Message, MessageWithTimestamp}
import com.bwsw.tstreamstransactionserver.options.CommitLogWriteSyncPolicy.{EveryNSeconds, EveryNewFile, EveryNth}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{CommitLogOptions, StorageOptions}
import org.slf4j.LoggerFactory

class ScheduledCommitLog(pathsToClosedCommitLogFiles: ArrayBlockingQueue[String], storageOptions: StorageOptions, commitLogOptions: CommitLogOptions, genFileID: => Long) extends Runnable with Time {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val commitLog = createCommitLog()
  private val maxIdleTimeBetweenRecordsMs = commitLogOptions.maxIdleTimeBetweenRecordsMs
  @volatile private var lastRecordTs = getCurrentTime
  @volatile private var currentCommitLogFile: String = _

  private def createCommitLog() = {
    val policy = commitLogOptions.commitLogWriteSyncPolicy match {
      case EveryNth => OnCountInterval(commitLogOptions.commitLogWriteSyncValue)
      case EveryNewFile => OnRotation
      case EveryNSeconds => OnTimeInterval(commitLogOptions.commitLogWriteSyncValue)
    }

    new CommitLog(Int.MaxValue, s"${storageOptions.path}${java.io.File.separatorChar}${storageOptions.commitLogDirectory}", policy, genFileID)
  }

  def putData(messageType: Byte, message: Message) = {
    val currentTime = getCurrentTime
    if (currentTime - lastRecordTs < maxIdleTimeBetweenRecordsMs || currentCommitLogFile == null)
    {
      this.synchronized{
        currentCommitLogFile = commitLog.putRec(MessageWithTimestamp(message, getCurrentTime).toByteArray, messageType)
      }
    } else {
      this.synchronized{
        val newCommitLogFile = commitLog.putRec(MessageWithTimestamp(message, getCurrentTime).toByteArray, messageType, startNew = true)
        if (logger.isDebugEnabled) logger.debug(s"Starting to write to the new commit log file: $newCommitLogFile")
        pathsToClosedCommitLogFiles.put(currentCommitLogFile)
        currentCommitLogFile = newCommitLogFile
      }
    }
    lastRecordTs = currentTime
    true
  }

  override def run(): Unit = {
    commitLog.close() foreach { path =>
      lastRecordTs = Long.MaxValue
      pathsToClosedCommitLogFiles.put(path)
    }
  }
}