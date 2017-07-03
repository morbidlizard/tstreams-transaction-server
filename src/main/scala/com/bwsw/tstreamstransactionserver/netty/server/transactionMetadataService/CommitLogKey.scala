package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService


class CommitLogKey(val id: Long)
  extends AnyVal
{
  def toByteArray: Array[Byte] = {
    val size = CommitLogKey.sizeInBytes

    val buffer = java.nio.ByteBuffer.allocate(size)
      .putLong(id)
    buffer.flip()

    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    bytes
  }
}

object CommitLogKey {
  private val sizeInBytes = java.lang.Long.BYTES

  def apply(id: Long): CommitLogKey = new CommitLogKey(id)

  def fromByteArray(bytes: Array[Byte]): CommitLogKey = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val id = buffer.getLong()
    CommitLogKey(id)
  }
}
