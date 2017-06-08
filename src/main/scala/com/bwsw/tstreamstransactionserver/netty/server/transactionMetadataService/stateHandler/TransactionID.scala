package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

class TransactionID(val id: Long) extends AnyVal {
  def toByteArray: Array[Byte] = {
    val size = java.lang.Long.BYTES
    val buffer = java.nio.ByteBuffer.allocate(size)
      .putLong(id)
    buffer.flip()

    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    bytes
  }

  override def toString: String = id.toString
}

object TransactionID {
  def apply(id: Long): TransactionID = new TransactionID(id)

  def fromByteArray(bytes: Array[Byte]): TransactionID = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val id = buffer.getLong
    TransactionID(id)
  }
}
