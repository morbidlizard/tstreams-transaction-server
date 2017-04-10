package com.bwsw.commitlog

import java.io._
import java.security.{DigestOutputStream, MessageDigest}
import java.util.Base64
import java.util.Base64.Encoder
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import javax.xml.bind.DatatypeConverter

import com.bwsw.commitlog.CommitLogFlushPolicy.{ICommitLogFlushPolicy, OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.commitlog.filesystem.FilePathManager

import scala.annotation.tailrec

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

  private val secondsInterval: Int = seconds

  private val base64Encoder: Encoder = Base64.getEncoder
  private val delimiter: Byte = 0
  @volatile private var fileCreationTime: Long = -1
  private val chunkWriteCount: AtomicInteger = new AtomicInteger(0)
  @volatile private var chunkOpenTime: Long = 0

  private val currentCommitLogFileToPut: AtomicReference[CommitLogFile] = new AtomicReference[CommitLogFile](null)
  private class CommitLogFile(path: String) {
    val absolutePath = new StringBuffer(path).append(FilePathManager.DATAEXTENSION).toString

    private val md5: MessageDigest = MessageDigest.getInstance("MD5")
    private def writeMD5File() = {
      val fileMD5 = DatatypeConverter.printHexBinary(md5.digest()).getBytes()
      new FileOutputStream(new StringBuffer(path).append(FilePathManager.MD5EXTENSION).toString) {
        write(fileMD5)
        close()
      }
    }

    private val outputStream = new BufferedOutputStream(new FileOutputStream(absolutePath, true))
    private val digestOutputStream = new DigestOutputStream(outputStream, md5)
    def put(encodedMsgWithType: Array[Byte]): Unit = digestOutputStream.write(encodedMsgWithType)

    @volatile var isClosed = false
    def flush(): Unit = {
      if (!isClosed) {
        digestOutputStream.flush()
        outputStream.flush()
      }
    }

    def close(): Boolean = {
      if (!isClosed) {
        isClosed = true
        digestOutputStream.on(false)
        digestOutputStream.flush()
        outputStream.flush()
        digestOutputStream.close()
        outputStream.close()
        writeMD5File()
        isClosed
      } else isClosed
    }
  }


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
  def putRec(message: Array[Byte], messageType: Byte, startNew: Boolean = false): String =  {

    // если хотим записать в новый файл при уже существующем коммит логе
    if (startNew && !firstRun) {
      resetCounters()
      val currentFile = currentCommitLogFileToPut.getAndSet(new CommitLogFile(s"$path${java.io.File.separatorChar}$nextFileID"))
      currentFile.close()
    }

    // если истекло время или мы начинаем записывать в новый коммит лог файл
    if (firstRun() || timeExceeded()) {
      if (!firstRun()) {
        resetCounters()
        currentCommitLogFileToPut.get().close()
      }
      fileCreationTime = getCurrentSecs()
      // TODO(remove this):write here chunkOpenTime or we will write first record instantly if OnTimeInterval policy set
      //      chunkOpenTime = System.currentTimeMillis()
      currentCommitLogFileToPut.set(new CommitLogFile(s"$path${java.io.File.separatorChar}$nextFileID"))
    }


    val now: Long = System.currentTimeMillis()
    policy match {
      case interval: OnTimeInterval if interval.seconds * 1000 + chunkOpenTime < now =>
        chunkOpenTime = now
        currentCommitLogFileToPut.get().flush()
      case interval: OnCountInterval if interval.count == chunkWriteCount.get() =>
        chunkWriteCount.set(0)
        currentCommitLogFileToPut.get().flush()
      case _ =>
    }

    val encodedMsgWithType: Array[Byte] =  delimiter +: base64Encoder.encode(messageType +: message)

    @tailrec
    def tryToPut(): String = {
      val currentFile = currentCommitLogFileToPut.get()
      scala.util.Try(currentFile.put(encodedMsgWithType)) match {
        case scala.util.Success(_) =>
          chunkWriteCount.incrementAndGet()
          currentFile.absolutePath
        case scala.util.Failure(_) =>
          tryToPut()
      }
    }
    tryToPut()
  }

  /** Finishes work with current file. */
  def close(): Option[String] =  {
    val currentFile = currentCommitLogFileToPut.get()
    if (!firstRun && currentFile != null) {
      resetCounters()
      currentFile.close()
      Some(currentFile.absolutePath)
    } else None
  }

  private def resetCounters(): Unit = {
    fileCreationTime = -1
    chunkWriteCount.set(0)
    chunkOpenTime = System.currentTimeMillis()
  }

  private def firstRun() = {
    fileCreationTime == -1
  }

  private def getCurrentSecs(): Long = {
    System.currentTimeMillis() / 1000
  }

  private def timeExceeded(): Boolean = {
    getCurrentSecs - fileCreationTime >= secondsInterval
  }
}