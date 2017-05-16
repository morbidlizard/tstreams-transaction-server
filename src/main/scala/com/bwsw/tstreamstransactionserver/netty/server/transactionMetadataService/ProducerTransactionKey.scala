package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService



case class ProducerTransactionKey(stream: Int, partition: Int, transactionID: Long) extends Ordered[ProducerTransactionKey]{


  override def compare(that: ProducerTransactionKey): Int = {
    if (this.stream < that.stream) -1
    else if (this.stream > that.stream) 1
    else if (this.partition < that.partition) -1
    else if (this.partition > that.partition) 1
    else if (this.transactionID < that.transactionID) -1
    else if (this.transactionID > that.transactionID) 1
    else 0
  }

  def toByteArray: Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(
      java.lang.Integer.BYTES +
      java.lang.Integer.BYTES +
      java.lang.Long.BYTES
    )
    buffer
      .putInt(stream)
      .putInt(partition)
      .putLong(transactionID)
      .array()
  }
}

object ProducerTransactionKey {
  def fromByteArray(bytes: Array[Byte]): ProducerTransactionKey = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val stream = buffer.getInt
    val partition = buffer.getInt
    val transactionID = buffer.getLong
    ProducerTransactionKey(stream, partition, transactionID)
  }
}




