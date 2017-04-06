package com.bwsw.commitlog.filesystem

import java.io.{BufferedInputStream, InputStream}
import java.util.Base64

import scala.collection.mutable.ArrayBuffer

class CommitLogStreamIterator(inputStream: InputStream) extends CommitLogIterator{
  protected val stream = new BufferedInputStream(inputStream)
  require {
    val begin = stream.read()
    begin == (0: Byte) || begin == -1
  }

  override def hasNext(): Boolean = {
    if (stream.available() > 0) true
    else false
  }

  override def close():Unit = {
    stream.close()
    inputStream.close()
  }

  override def next(): Array[Byte] = {
    if (!hasNext()) throw new NoSuchElementException

    val record = new ArrayBuffer[Byte]()
    var byte = -1
    while ( {
      byte = stream.read()
      byte != -1 && byte != 0
    }) {
      record += byte.toByte
    }
    Base64.getDecoder.decode(record.toArray)
  }
}

