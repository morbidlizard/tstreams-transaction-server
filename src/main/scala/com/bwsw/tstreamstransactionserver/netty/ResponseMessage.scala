package com.bwsw.tstreamstransactionserver.netty
import ResponseMessage._
import io.netty.buffer.ByteBuf

case class ResponseMessage(id: Long, body: Array[Byte]) {
  /** Serializes a message. */
  def toByteArray: Array[Byte] = {
    val length = body.length

    val size =
      headerFieldSize +
        lengthFieldSize +
        length

    val buffer =
      java.nio.ByteBuffer
        .allocate(size)
        .putLong(id)
        .putInt(length)
        .put(body)
    buffer.flip()

    val bytes: Array[Byte] = {
      val array = new Array[Byte](size)
      buffer.get(array)
      array
    }
    bytes
  }
}

object ResponseMessage {
  val headerFieldSize: Int =
      java.lang.Long.BYTES  //id

  val lengthFieldSize =
    java.lang.Integer.BYTES //length

  def fromByteArray(bytes: Array[Byte]): ResponseMessage = {
    val buffer = java.nio.ByteBuffer
      .wrap(bytes)

    val id = buffer.getLong
    val bodyLength = buffer.getInt
    val body = {
      val bytes = new Array[Byte](bodyLength)
      buffer.get(bytes)
      bytes
    }

    ResponseMessage(id, body)
  }

  def fromByteBuf(buf: ByteBuf): ResponseMessage = {
    val id = buf.readLong()
    val bodyLength = buf.readInt()

    val bodyInBytes = {
      val bytes = new Array[Byte](bodyLength)
      buf.slice()
      buf.readBytes(bytes)
      bytes
    }

    ResponseMessage(id, bodyInBytes)
  }

  def getIdFromByteBuf(buf: ByteBuf): Long = {
    buf.getLong(0)
  }
}
