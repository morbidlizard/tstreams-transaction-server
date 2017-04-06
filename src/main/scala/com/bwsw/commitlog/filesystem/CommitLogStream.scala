package com.bwsw.commitlog.filesystem

import java.io.ByteArrayInputStream
import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter

class CommitLogStream(id: Long, content: Array[Byte], md5: Option[Array[Byte]]) extends CommitLogStorage{
  require(if (md5.isDefined) md5.get.length == 32 else true)

  override def getID: Long = id

  /** Returns true if md5-file exists. */
  override def md5Exists(): Boolean = md5.isDefined

  /** Returns an iterator over records */
  override def getIterator: CommitLogIterator = new CommitLogStreamIterator(new ByteArrayInputStream(content))

  /** Returns existing MD5 of this file. Throws an exception otherwise. */
  override def getMD5: Array[Byte] = if (!md5Exists()) throw new IllegalArgumentException("There is no md5 sum!") else md5.get

  /** bytes to read from this file */
  private val chunkSize = 100000

  /** Returns calculated MD5 of this file. */
  override def calculateMD5(): Array[Byte] = {
    val stream = new ByteArrayInputStream(content)

    val md5: MessageDigest = MessageDigest.getInstance("MD5")
    md5.reset()
    while (stream.available() > 0) {
      val chunk = new Array[Byte](chunkSize)
      val bytesRead = stream.read(chunk)
      md5.update(chunk.take(bytesRead))
    }

    stream.close()

    DatatypeConverter.printHexBinary(md5.digest()).getBytes
  }
}
