package com.bwsw.tstreamstransactionserver.netty


/** Message is a placeholder for some binary information.
  *
  *  @constructor create a new message with body, the size of body, and a protocol to serialize/deserialize the body.
  *  @param length a size of body.
  *  @param protocol a protocol to serialize/deserialize the body.
  *  @param body a binary representation of information.
  *
  */
case class Message(id: Long, length: Int, protocol: Byte, body: Array[Byte], token: Int, method: Byte)
{
  /** Serializes a message. */
  def toByteArray: Array[Byte] = java.nio.ByteBuffer
    .allocate(Message.headerSize + body.length)
    .putInt(length)
    .putLong(id)
    .put(protocol)
    .putInt(token)
    .put(method)
    .put(body)
    .array()
}
object Message {
  val headerSize: Byte = (
    java.lang.Integer.BYTES +    //length
      java.lang.Long.BYTES +     //id
      java.lang.Byte.BYTES +     //protocol
      java.lang.Integer.BYTES +  //token
      java.lang.Byte.BYTES       //method
    ).toByte
  /** Deserializes a binary to message. */
  def fromByteArray(bytes: Array[Byte]): Message = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val length = buffer.getInt
    val id     = buffer.getLong
    val protocol = buffer.get
    val token = buffer.getInt
    val method = buffer.get()
    val message = {
      val bytes = new Array[Byte](buffer.limit() - headerSize)
      buffer.get(bytes)
      bytes
    }
    Message(id, length, protocol, message, token, method)
  }
}

