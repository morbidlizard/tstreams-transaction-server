package com.bwsw.commitlog

import java.io._
import java.math.BigInteger
import java.security.MessageDigest
import java.util.Base64
import java.util.Base64.Encoder

import com.bwsw.commitlog.CommitLogFlushPolicy.{ICommitLogFlushPolicy, OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.commitlog.filesystem.FilePathManager

object CommitLog {
  val MD5EXTENSION = ".md5"
}

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
class CommitLog(seconds: Int, path: String, policy: ICommitLogFlushPolicy = OnRotation) {
  require(seconds > 0, "Seconds cannot be less than 1")

  private val secondsInterval: Int = seconds
  private val filePathManager: FilePathManager = new FilePathManager(path)
  private val base64Encoder: Encoder = Base64.getEncoder
  private val delimiter: Byte = 0
  @volatile private var fileCreationTime: Long = -1
  private var chunkWriteCount: Int = 0
  private var chunkOpenTime: Long = 0

  private var currentCommitLogFileToPut: CommitLogFile = _
  private class CommitLogFile(path: String) {
    val absolutePath = new StringBuffer(path).append(FilePathManager.EXTENSION).toString

    private val md5: MessageDigest = MessageDigest.getInstance("MD5")
    private def writeMD5File() = {
      val fileMD5 = new BigInteger(1, md5.digest()).toByteArray

      new FileOutputStream(new StringBuffer(path).append(CommitLog.MD5EXTENSION).toString) {
        write(fileMD5)
        close()
      }
    }
    private def updateMD5Digest(bytes: Array[Byte]) = md5.update(bytes)

    private val outputStream = new BufferedOutputStream(new FileOutputStream(absolutePath, true))
    def put(encodedMsgWithType: Array[Byte]):Unit = this.synchronized{
      val data = delimiter +: encodedMsgWithType
      outputStream.write(data)
      updateMD5Digest(data)
    }

    def flush() = outputStream.flush()

    def close(): Unit = {
      outputStream.flush()
      outputStream.close()
      writeMD5File()
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
  def putRec(message: Array[Byte], messageType: Byte, startNew: Boolean = false): String = {

    // если хотим записать в новый файл при уже существующем коммит логе
    if (startNew && !firstRun) {
      resetCounters()
      currentCommitLogFileToPut.close()
      currentCommitLogFileToPut = new CommitLogFile(filePathManager.getNextPath())
    }

    // если истекло время или мы начинаем записывать в новый коммит лог файл
    if (firstRun() || timeExceeded()) {
      if (!firstRun()) {
        resetCounters()
        currentCommitLogFileToPut.close()
      }
      fileCreationTime = getCurrentSecs()
      // TODO(remove this):write here chunkOpenTime or we will write first record instantly if OnTimeInterval policy set
      //      chunkOpenTime = System.currentTimeMillis()
      currentCommitLogFileToPut = new CommitLogFile(filePathManager.getNextPath())
    }

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

    val encodedMsgWithType: Array[Byte] = base64Encoder.encode(messageType +: message)
    currentCommitLogFileToPut.put(encodedMsgWithType)

    chunkWriteCount += 1

    currentCommitLogFileToPut.absolutePath
  }

  /** Finishes work with current file. */
  def close(): Unit = {
    if (!firstRun) {
      resetCounters()
      currentCommitLogFileToPut.close()
    }
  }

  //  /** Return decoded messages from specified file.
  //    *
  //    * @param path path to file to read data from.
  //    * @return sequence of decoded messages.
  //    */
  //  def getMessages(path: String): IndexedSeq[Array[Byte]] = {
  //    val base64decoder: Decoder = Base64.getDecoder
  //    val byteArray = Files.readAllBytes(Paths.get(path))
  //    var msgs: IndexedSeq[Array[Byte]] = IndexedSeq[Array[Byte]]()
  //    var i = 0
  //    while (i < byteArray.length) {
  //      var msg: Array[Byte] = Array[Byte]()
  //      if (byteArray(i) == 0) {
  //        i += 1
  //        while (i < byteArray.length && byteArray(i) != 0.toByte) {
  //          msg = msg :+ byteArray(i)
  //          i += 1
  //        }
  //        msgs = msgs :+ base64decoder.decode(msg)
  //      } else {
  //        new Exception("No zero at the beginning of a message")
  //      }
  //    }
  //    return msgs
  //  }

  //  /** Performance test.
  //    *
  //    * Writes specified count of messages to file.
  //    *
  //    * @param countOfRecords count of records to write.
  //    * @param message message to write.
  //    * @param typeOfMessage type of message.
  //    * @return count of milliseconds writing to file took.
  //    */
  //  def perf(countOfRecords: Int, message: Array[Byte], typeOfMessage: Byte): Long = {
  //    require(countOfRecords > 0, "Count of records cannot be less than 1")
  //
  //    val before = System.currentTimeMillis()
  //    for (i <- 1 to countOfRecords) putRec(message, typeOfMessage, startNew = false)
  //    System.currentTimeMillis() - before
  //  }

  //  private def flushStream() = {
  //    outputStream.flush()
  //  }

  private def resetCounters() = {
    fileCreationTime = -1
    chunkWriteCount = 0
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
