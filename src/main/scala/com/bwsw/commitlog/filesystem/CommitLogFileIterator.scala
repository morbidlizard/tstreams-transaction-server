package com.bwsw.commitlog.filesystem

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Base64

import scala.collection.mutable.ArrayBuffer

/** Iterator over records of the commitlog file.
  *
  * @param path full path to file
  */
class CommitLogFileIterator(path: String) extends CommitLogIterator {
  private val fileInputStream = new FileInputStream(new File(path))
  override protected val stream = new BufferedInputStream(fileInputStream)
  require {
    val begin = stream.read()
    begin == (0: Byte) || begin == -1
  }

  override def close():Unit = {
    stream.close()
    fileInputStream.close()
  }
}
