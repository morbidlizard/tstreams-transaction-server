package com.bwsw.commitlog.filesystem

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Base64

import scala.collection.mutable.ArrayBuffer

/** Iterator over records of the commitlog file.
  *
  * @param path full path to file
  */
class CommitLogFileIterator(path: String) extends Iterator[Array[Byte]] {
  private val fileInputStream = new FileInputStream(new File(path))
  private val stream = new BufferedInputStream(fileInputStream)
  require {
    val begin = stream.read()
    begin == (0: Byte) || begin == -1
  }

  override def hasNext(): Boolean = {
    if (stream.available() > 0) true
    else {
      stream.close()
      fileInputStream.close()
      false
    }
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
