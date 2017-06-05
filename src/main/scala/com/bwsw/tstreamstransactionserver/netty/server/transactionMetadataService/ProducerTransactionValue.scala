package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

case class ProducerTransactionValue(state: TransactionStates,
                                    quantity: Int,
                                    ttl: Long,
                                    timestamp: Long
                                   )
  extends Ordered[ProducerTransactionValue]
{

  def toByteArray: Array[Byte] = {
    val size = ProducerTransactionValue.sizeInBytes

    val buffer = java.nio.ByteBuffer.allocate(size)
      .putInt(state.value)
      .putInt(quantity)
      .putLong(ttl)
      .putLong(timestamp)
    buffer.flip()

    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    bytes
  }

  override def compare(that: ProducerTransactionValue): Int = {
    if (this.timestamp < that.timestamp) -1
    else if (this.timestamp > that.timestamp) 1
    else if (this.state.value < that.state.value) -1
    else if (this.state.value > that.state.value) 1
    else 0
  }
}

object ProducerTransactionValue
{
  private val sizeInBytes = java.lang.Integer.BYTES +
    java.lang.Integer.BYTES +
    java.lang.Long.BYTES +
    java.lang.Long.BYTES

  def fromByteArray(bytes: Array[Byte]): ProducerTransactionValue = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val state = TransactionStates(buffer.getInt)
    val quantity = buffer.getInt
    val ttl = buffer.getLong
    val timestamp = buffer.getLong
    ProducerTransactionValue(state, quantity, ttl, timestamp)
  }
}
