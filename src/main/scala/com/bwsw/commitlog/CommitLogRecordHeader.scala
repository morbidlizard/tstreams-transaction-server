package com.bwsw.commitlog

import java.nio.ByteBuffer

private[commitlog] case class CommitLogRecordHeader(id: Long, messageType: Byte, messageLength: Int, timestamp: Long)

object CommitLogRecordHeader{
  private[commitlog] def fromByteArray(binaryHeader: Array[Byte]) = {
    val buffer = ByteBuffer.wrap(binaryHeader)
    val id     = buffer.getLong()
    val messageType   = buffer.get()
    val messageLength = buffer.getInt()
    val timestamp     = buffer.getLong()
    CommitLogRecordHeader(id, messageType, messageLength, timestamp)
  }
}
