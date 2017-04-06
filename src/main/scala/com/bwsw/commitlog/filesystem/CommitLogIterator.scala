package com.bwsw.commitlog.filesystem

import java.io.BufferedInputStream
import java.util.Base64

import scala.collection.mutable.ArrayBuffer

abstract class CommitLogIterator extends Iterator[Array[Byte]] {
  protected val stream: BufferedInputStream

  override def hasNext(): Boolean = {
    if (stream.available() > 0) true
    else false
  }

  def close():Unit = {
    stream.close()
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
