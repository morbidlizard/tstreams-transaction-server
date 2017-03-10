package com.bwsw.tstreamstransactionserver.netty

case class MessageWithTimestamp(message: Message, timestamp: Long = System.currentTimeMillis()){
  def toByteArray: Array[Byte] = java.nio.ByteBuffer.allocate(8).putLong(timestamp).array() ++ message.toByteArray
}

object MessageWithTimestamp {
  def fromByteArray(bytes: Array[Byte]): MessageWithTimestamp = {
    val (timestamp, message) = bytes.splitAt(8)
    MessageWithTimestamp(Message.fromByteArray(message), java.nio.ByteBuffer.wrap(timestamp).getLong)
  }
}
