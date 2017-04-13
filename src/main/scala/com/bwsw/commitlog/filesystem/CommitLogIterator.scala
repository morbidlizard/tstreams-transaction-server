package com.bwsw.commitlog.filesystem

import java.io.BufferedInputStream

import com.bwsw.commitlog.CommitLogRecord

import scala.collection.mutable.ArrayBuffer

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
      val record = new ArrayBuffer[Byte]()
      var byte = -1
      while ( {
        byte = stream.read()
        byte != -1 && byte != 0
      }) {
        record += byte.toByte
      }
      CommitLogRecord.fromByteArrayWithoutDelimiter(record.toArray) match {
        case scala.util.Left(_) => Left(new NoSuchElementException("There is no next commit log record!"))
        case scala.util.Right(record) => Right(record)
      }
    }
  }
}
