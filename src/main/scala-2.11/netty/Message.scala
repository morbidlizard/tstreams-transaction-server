package netty

import netty.Descriptor.Descriptor

case class Message(length: Int, descriptor: Descriptor, message: Array[Byte])
{
  def toByteArray: Array[Byte] = java.nio.ByteBuffer
    .allocate(Message.headerSize + message.length)
    .putInt(length)
    .put(descriptor.id.toByte)
    .put(message)
    .array()

  override def toString: String = s"message length: $length, method: $descriptor"
}
object Message {
  val headerSize: Byte = 5
  def fromByteArray(bytes: Array[Byte]): Message = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val length = buffer.getInt
    val descriptor = buffer.get
    val message = {
      val bytes = new Array[Byte](buffer.limit() - headerSize)
      buffer.get(bytes)
      bytes
    }
    Message(length, Descriptor(descriptor), message)
  }
}

