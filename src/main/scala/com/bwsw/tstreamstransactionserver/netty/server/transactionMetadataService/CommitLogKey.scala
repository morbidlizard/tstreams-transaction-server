package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService


class CommitLogKey(val id: Long) extends AnyVal{
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
      .putLong(id)
      .array()
  }
}

object CommitLogKey {
  def apply(id: Long): CommitLogKey = new CommitLogKey(id)

  def fromByteArray(bytes: Array[Byte]): CommitLogKey = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val id = buffer.getLong()
    CommitLogKey(id)
  }
}
