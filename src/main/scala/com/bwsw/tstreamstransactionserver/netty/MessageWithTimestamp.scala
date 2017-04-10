package com.bwsw.tstreamstransactionserver.netty

case class MessageWithTimestamp(message: Message, timestamp: Long, id: Long){
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer.allocate(java.lang.Long.BYTES*2)
      .putLong(timestamp)
      .putLong(id)
      .array() ++: message.toByteArray
  }
}

object MessageWithTimestamp {
  def fromByteArray(bytes: Array[Byte]): MessageWithTimestamp = {
    val (timestampAndID, message) = bytes.splitAt(java.lang.Long.BYTES*2)
    val buffer = java.nio.ByteBuffer.wrap(timestampAndID)
    MessageWithTimestamp(Message.fromByteArray(message), buffer.getLong, buffer.getLong)
  }
}
