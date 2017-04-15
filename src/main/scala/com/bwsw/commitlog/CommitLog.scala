package com.bwsw.commitlog

import java.io._
import java.security.{DigestOutputStream, MessageDigest}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import javax.xml.bind.DatatypeConverter

import com.bwsw.commitlog.CommitLogFlushPolicy.{ICommitLogFlushPolicy, OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.commitlog.filesystem.FilePathManager


/** Logger which stores records continuously in files in specified location.
  *
  * Stores data in files placed YYYY/mm/dd/{serial number}.dat. If it works correctly, md5-files placed
  * YYYY/mm/dd/{serial number}.md5 shall be generated as well. New file starts on user request or when configured time
  * was exceeded.
  *
  * @param seconds period of time to write records into the same file, then start new file
  * @param path location to store files at
  * @param policy policy to flush data into file (OnRotation by default)
  */
class CommitLog(seconds: Int, path: String, policy: ICommitLogFlushPolicy = OnRotation, nextFileID: => Long) {
  require(seconds > 0, "Seconds cannot be less than 1")
  private val millisInterval: Long = TimeUnit.SECONDS.toMillis(seconds)

  private var chunkWriteCount: Int = 0
  private var chunkOpenTime: Long = 0

  private val pathWithSeparator = s"$path${java.io.File.separatorChar}"
  private class CommitLogFile(val id: Long) {
    private[CommitLog] val absolutePath: String = new StringBuilder(pathWithSeparator)
      .append(id).append(FilePathManager.DATAEXTENSION).toString
    private val recordIDGen = new AtomicLong(0L)

    private val md5: MessageDigest = MessageDigest.getInstance("MD5")

    private def writeMD5File() = {
      val fileMD5 = DatatypeConverter.printHexBinary(md5.digest()).getBytes
      new FileOutputStream(new StringBuilder(pathWithSeparator)
        .append(id).append(FilePathManager.MD5EXTENSION).toString) {
        write(fileMD5)
        close()
      }
    }

    private val fileStream = new FileOutputStream(absolutePath)
    private val outputStream = new BufferedOutputStream(fileStream)
    private val digestOutputStream = new DigestOutputStream(outputStream, md5)
    private[CommitLog] val creationTime: Long = System.currentTimeMillis()

    private[CommitLog] def put(messageType: Byte, message: Array[Byte]): Unit = {
      val commitLogRecord = CommitLogRecord(recordIDGen.getAndIncrement(), messageType, message)
      val recordToBinary = commitLogRecord.toByteArrayWithDelimiter
      digestOutputStream.write(recordToBinary)
    }

    private[CommitLog] def flush(): Unit = {
      digestOutputStream.flush()
      outputStream.flush()
      fileStream.flush()
    }

    private[CommitLog] def close(withMD5: Boolean = true): Unit = {
      digestOutputStream.on(false)
      digestOutputStream.close()
      outputStream.close()
      fileStream.close()
      if (withMD5) {
        writeMD5File()
      }
    }
  }

  private var currentCommitLogFileToPut: CommitLogFile = new CommitLogFile(nextFileID)
  /** Puts record and its type to an appropriate file.
    *
    * Writes data to file in format (delimiter)(BASE64-encoded type and message). When writing to one file finished,
    * md5-sum file generated.
    *
    * @param message message to store
    * @param messageType type of message to store
    * @param startNew start new file if true
    * @return name of file record was saved in
    */
  def putRec(message: Array[Byte], messageType: Byte, startNew: Boolean = false): String = this.synchronized{
    val now: Long = System.currentTimeMillis()
    policy match {
      case interval: OnTimeInterval if interval.seconds * 1000 + chunkOpenTime < now =>
        chunkOpenTime = now
        currentCommitLogFileToPut.flush()
      case interval: OnCountInterval if interval.count == chunkWriteCount =>
        chunkWriteCount = 0
        currentCommitLogFileToPut.flush()
      case _ =>
    }

    // If we want to open new File or if time between creation new files exceeds `millisInterval`
    if (startNew || timeExceeded()) close()

    currentCommitLogFileToPut.put(messageType, message)
    chunkWriteCount += 1

    currentCommitLogFileToPut.absolutePath
  }

  /** Finishes work with current file. */
  def close(createNewFile: Boolean = true, withMD5: Boolean = true): String = this.synchronized {
    val currentCommitLogFile = currentCommitLogFileToPut
    val path = currentCommitLogFile.absolutePath
    if (createNewFile) {
      currentCommitLogFileToPut = new CommitLogFile(nextFileID)
    }
    currentCommitLogFile.close()
    resetCounters()
    path
  }

  final def currentFileID: Long = currentCommitLogFileToPut.id

  private def resetCounters(): Unit = {
    chunkWriteCount = 0
    chunkOpenTime = System.currentTimeMillis()
  }

  private def timeExceeded(): Boolean = {
    (System.currentTimeMillis() - currentCommitLogFileToPut.creationTime) >= millisInterval
  }
}