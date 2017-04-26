package com.bwsw.commitlog.filesystem

import java.io.{BufferedInputStream, InputStream}
import java.util.Base64

import scala.collection.mutable.ArrayBuffer

class CommitLogBinaryIterator(inputStream: InputStream) extends CommitLogIterator{
  protected val stream = new BufferedInputStream(inputStream)

  override def close():Unit = {
    stream.close()
    inputStream.close()
  }
}

