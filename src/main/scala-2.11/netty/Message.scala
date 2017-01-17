package netty



case class Message(length: Int, protocol: Byte, body: Array[Byte])
{
  def toByteArray: Array[Byte] = java.nio.ByteBuffer
    .allocate(Message.headerSize + body.length)
    .putInt(length)
    .put(protocol)
    .put(body)
    .array()

  override def toString: String = s"message length: $length"
}
object Message {
  val headerSize: Byte = 5
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

