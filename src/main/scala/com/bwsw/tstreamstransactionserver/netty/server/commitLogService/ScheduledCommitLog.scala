package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.ArrayBlockingQueue

import com.bwsw.commitlog.CommitLog
import com.bwsw.commitlog.CommitLogFlushPolicy.{OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.tstreamstransactionserver.netty.{Message, MessageWithTimestamp}
import com.bwsw.tstreamstransactionserver.options.CommitLogWriteSyncPolicy.{EveryNSeconds, EveryNewFile, EveryNth}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{CommitLogOptions, StorageOptions}

class ScheduledCommitLog(pathsToClosedCommitLogFiles: ArrayBlockingQueue[String], storageOptions: StorageOptions, commitLogOptions: CommitLogOptions) {
  private val commitLog = createCommitLog()
  private val maxIdleTimeBetweenRecords = commitLogOptions.maxIdleTimeBetweenRecords * 1000
  private var lastRecordTs = System.currentTimeMillis()
  private var currentCommitLogFile: String = _

  private def createCommitLog() = {
    val policy = commitLogOptions.commitLogWriteSyncPolicy match {
      case EveryNth => OnCountInterval(commitLogOptions.commitLogWriteSyncValue)
      case EveryNewFile => OnRotation
      case EveryNSeconds => OnTimeInterval(commitLogOptions.commitLogWriteSyncValue)
    }

    new CommitLog(Int.MaxValue, storageOptions.path, policy)
  }

  def putData(messageType: Byte, message: Message) = {
    val currentTime = System.currentTimeMillis()
    if (currentTime - lastRecordTs < maxIdleTimeBetweenRecords || currentCommitLogFile == null)
    {
      this.synchronized({
        currentCommitLogFile = commitLog.putRec(MessageWithTimestamp(message).toByteArray, messageType)
      })
    } else {
      this.synchronized({
        val newCommitLogFile = commitLog.putRec(MessageWithTimestamp(message).toByteArray, messageType, startNew = true)
        pathsToClosedCommitLogFiles.put(currentCommitLogFile)
        currentCommitLogFile = newCommitLogFile
      })
    }
    lastRecordTs = currentTime
    true
  }
}