package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.PriorityBlockingQueue

import com.bwsw.commitlog.CommitLog
import com.bwsw.commitlog.CommitLogFlushPolicy.{OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.commitlog.filesystem.{CommitLogFile, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.netty.server.Time
import com.bwsw.tstreamstransactionserver.options.CommitLogWriteSyncPolicy.{EveryNSeconds, EveryNewFile, EveryNth}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{CommitLogOptions, StorageOptions}
import org.slf4j.LoggerFactory

class ScheduledCommitLog(pathsToClosedCommitLogFiles: PriorityBlockingQueue[CommitLogStorage],
                         storageOptions: StorageOptions,
                         commitLogOptions: CommitLogOptions,
                         genFileID: => Long
                        ) extends Runnable with Time {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val commitLog = createCommitLog()


  private def createCommitLog() = {
    val policy = commitLogOptions.syncPolicy match {
      case EveryNth => OnCountInterval(commitLogOptions.syncValue)
      case EveryNewFile => OnRotation
      case EveryNSeconds => OnTimeInterval(commitLogOptions.syncValue)
    }
    new CommitLog(Int.MaxValue, s"${storageOptions.path}${java.io.File.separatorChar}${storageOptions.commitLogRawDirectory}", policy, genFileID)
  }

  def currentCommitLogFile: Long = commitLog.currentFileID

  def putData(messageType: Byte, message: Array[Byte]) = {
    commitLog.putRec(message, messageType)
    true
  }

  def closeWithoutCreationNewFile() = {
    val path = commitLog.close(createNewFile = false)
    pathsToClosedCommitLogFiles.put(new CommitLogFile(path))
  }

  override def run(): Unit = {
    val path = commitLog.close()
    pathsToClosedCommitLogFiles.put(new CommitLogFile(path))
  }
}