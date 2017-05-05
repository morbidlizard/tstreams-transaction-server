package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler


case class KeyStreamPartition(stream: Int, partition: Int) {

  def toByteArray: Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(
      java.lang.Integer.BYTES + java.lang.Integer.BYTES
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
    val stream = buffer.getInt
    val partition = buffer.getInt
    KeyStreamPartition(stream, partition)
  }
}
