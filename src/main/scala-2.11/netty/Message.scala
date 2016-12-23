package netty



case class Message(length: Int, body: Array[Byte])
{
  def toByteArray: Array[Byte] = java.nio.ByteBuffer
    .allocate(Message.headerSize + body.length)
    .putInt(length)
    .put(body)
    .array()

  override def toString: String = s"message length: $length"
}
object Message {
  val headerSize: Byte = 4
  def fromByteArray(bytes: Array[Byte]): Message = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val length = buffer.getInt
    val message = {
      val bytes = new Array[Byte](buffer.limit() - headerSize)
      buffer.get(bytes)
      bytes
    }
    Message(length, message)
  }
}

