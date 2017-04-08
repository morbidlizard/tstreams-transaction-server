package com.bwsw.commitlog.filesystem

import java.io.{BufferedInputStream, InputStream}
import java.util.Base64

import scala.collection.mutable.ArrayBuffer

class CommitLogBinaryIterator(inputStream: InputStream) extends CommitLogIterator{
  protected val stream = new BufferedInputStream(inputStream)
  require {
    val begin = stream.read()
    begin == (0: Byte) || begin == -1
  }

  override def close():Unit = {
    stream.close()
    inputStream.close()
  }
}

