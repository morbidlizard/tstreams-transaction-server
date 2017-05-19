package com.bwsw.tstreamstransactionserver.netty.server.consumerService

case class ConsumerTransactionValue(transactionId: Long, timestamp:Long) {
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer
      .allocate(java.lang.Long.BYTES + java.lang.Long.BYTES)
      .putLong(transactionId)
      .putLong(timestamp)
      .array()
  }
}

object ConsumerTransactionValue {
  def fromByteArray(bytes: Array[Byte]): ConsumerTransactionValue = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val transactionId = buffer.getLong
    val timestamp = buffer.getLong
    ConsumerTransactionValue(transactionId, timestamp)
  }
}
