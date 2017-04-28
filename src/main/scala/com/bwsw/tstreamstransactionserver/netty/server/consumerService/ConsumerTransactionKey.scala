package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import java.nio.charset.StandardCharsets

case class ConsumerTransactionKey(name: String, stream: java.lang.Long, partition: java.lang.Integer) {
  def toByteArray: Array[Byte] = {
    val nameBinary = name.getBytes(ConsumerTransactionKey.charset)
    val nameFieldSize = java.lang.Integer.BYTES
    val buffer = java.nio.ByteBuffer.allocate(
      nameFieldSize + nameBinary.length + java.lang.Long.BYTES + java.lang.Integer.BYTES)

    buffer
      .putInt(nameBinary.length)
      .put(nameBinary)
      .putLong(stream)
      .putInt(partition)
      .array()
  }

}

object ConsumerTransactionKey {
  val charset = StandardCharsets.UTF_8

  def fromByteArray(bytes: Array[Byte]): ConsumerTransactionKey = {
    val buffer     = java.nio.ByteBuffer.wrap(bytes)
    val nameLength = buffer.getInt
    val name = {
      val bytes = new Array[Byte](nameLength)
      buffer.get(bytes)
      new String(bytes, charset)
    }
    val stream = buffer.getLong
    val partition = buffer.getInt
    ConsumerTransactionKey(name, stream, partition)
  }
}
