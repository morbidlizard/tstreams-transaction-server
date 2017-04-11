package com.bwsw.tstreamstransactionserver.netty

case class MessageWithTimestamp(message: Message, timestamp: Long){
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
      .putLong(timestamp)
      .array() ++: message.toByteArray
  }
}

object MessageWithTimestamp {
  def fromByteArray(bytes: Array[Byte]): MessageWithTimestamp = {
    val (timestamp, message) = bytes.splitAt(java.lang.Long.BYTES)
    val buffer = java.nio.ByteBuffer.wrap(timestamp)
    MessageWithTimestamp(Message.fromByteArray(message), buffer.getLong)
  }
}
