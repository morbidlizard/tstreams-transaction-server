package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

class TransactionID(val id: Long) extends AnyVal {
  def toByteArray: Array[Byte] = {
    java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
      .putLong(id)
      .array()
  }
}

object TransactionID {
  def apply(id: Long): TransactionID = new TransactionID(id)

  def fromByteArray(bytes: Array[Byte]): TransactionID = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val id = buffer.getLong
    TransactionID(id)
  }
}
