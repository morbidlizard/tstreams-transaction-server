package com.bwsw.tstreamstransactionserver.netty.server.streamService


class StreamKey(val id: Int) extends AnyVal {
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer.allocate(java.lang.Integer.BYTES)
      .putInt(id)
      .array()
  }
}

object StreamKey {
  def apply(id: Int): StreamKey = new StreamKey(id)

  def fromByteArray(bytes: Array[Byte]) : StreamKey = {
    val id = java.nio.ByteBuffer.wrap(bytes).getInt
    StreamKey(id)
  }
}
