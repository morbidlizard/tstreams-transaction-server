package com.bwsw.commitlog.filesystem

import java.io.BufferedInputStream

import com.bwsw.commitlog.{CommitLogRecord, CommitLogRecordHeader}
import CommitLogIterator.EOF

abstract class CommitLogIterator extends Iterator[Either[NoSuchElementException, CommitLogRecord]] {
  protected val stream: BufferedInputStream

  override def hasNext(): Boolean = {
    if (stream.available() > 0) true
    else false
  }

  def close():Unit = {
    stream.close()
  }

  override def next(): Either[NoSuchElementException, CommitLogRecord] = {
    if (!hasNext()) Left(new NoSuchElementException("There is no next commit log record!"))
    else {
      val recordWithoutMessage = new Array[Byte](CommitLogRecord.headerSize)
      var byte = stream.read(recordWithoutMessage)
      if (byte != EOF && byte == CommitLogRecord.headerSize) {
        val header = CommitLogRecordHeader.fromByteArray(recordWithoutMessage)
        val message = new Array[Byte](header.messageLength)
        byte = stream.read(message)
        if (byte != EOF && byte == header.messageLength) {
          Right(CommitLogRecord(header.id, header.messageType, message, header.timestamp))
        } else {
          Left(new NoSuchElementException("There is no next commit log record!"))
        }
      } else {
        Left(new NoSuchElementException("There is no next commit log record!"))
      }
    }
  }
}

private object CommitLogIterator {
  val EOF:Int = -1
}
