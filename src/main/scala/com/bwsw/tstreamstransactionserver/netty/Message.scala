package com.bwsw.tstreamstransactionserver.netty


/** Message is a placeholder for some binary information.
  *
  *  @constructor create a new message with body, the size of body, and a protocol to serialize/deserialize the body.
  *  @param length a size of body.
  *  @param protocol a protocol to serialize/deserialize the body.
  *  @param body a binary representation of information.
  *
  */
case class Message(length: Int, protocol: Byte, body: Array[Byte])
{
  /** Serializes a message. */
  def toByteArray: Array[Byte] = java.nio.ByteBuffer
    .allocate(Message.headerSize + body.length)
    .putInt(length)
    .put(protocol)
    .put(body)
    .array()

  override def toString: String = s"message length: $length"
}
object Message {
  /** The size of sum of length and protocol fields. */
  val headerSize: Byte = 5
  /** Deserializes a binary to message. */
  def fromByteArray(bytes: Array[Byte]): Message = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val length = buffer.getInt
    val protocol = buffer.get
    val message = {
      val bytes = new Array[Byte](buffer.limit() - headerSize)
      buffer.get(bytes)
      bytes
    }
    Message(length, protocol, message)
  }
}

