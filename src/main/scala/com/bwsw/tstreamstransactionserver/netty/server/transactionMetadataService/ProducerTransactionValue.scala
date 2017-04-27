package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
case class ProducerTransactionValue(state: TransactionStates, quantity: Int, ttl: Long, timestamp: Long) {
  def toByteArray: Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(
      java.lang.Integer.BYTES +
        java.lang.Integer.BYTES +
        java.lang.Long.BYTES +
        java.lang.Long.BYTES
    )
    buffer
      .putInt(state.value)
      .putInt(quantity)
      .putLong(ttl)
      .putLong(timestamp)
      .array()
  }
}

object ProducerTransactionValue
{
  def fromByteArray(bytes: Array[Byte]): ProducerTransactionValue = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val state = TransactionStates(buffer.getInt)
    val quantity = buffer.getInt
    val ttl = buffer.getLong
    val timestamp = buffer.getLong
    ProducerTransactionValue(state, quantity, ttl, timestamp)
  }
}
