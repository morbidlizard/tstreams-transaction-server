package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler


case class KeyStreamPartition(stream: Long, partition: Int) {

  def toByteArray: Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(
      java.lang.Long.BYTES + java.lang.Integer.BYTES
    )
    buffer
      .putLong(stream)
      .putInt(partition)
      .array()
  }
}

object KeyStreamPartition {
  def fromByteArray(bytes: Array[Byte]): KeyStreamPartition = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val stream = buffer.getLong
    val partition = buffer.getInt
    KeyStreamPartition(stream, partition)
  }
}
