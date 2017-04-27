package com.bwsw.tstreamstransactionserver.netty.server.streamService


class StreamKey(val id: Long) extends AnyVal {
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
      .putLong(id)
      .array()
  }
}

object StreamKey {
  def apply(id: Long): StreamKey = new StreamKey(id)

  def fromByteArray(bytes: Array[Byte]) : StreamKey = {
    val id = java.nio.ByteBuffer.wrap(bytes).getLong
    StreamKey(id)
  }
}
