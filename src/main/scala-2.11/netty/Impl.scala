package netty

object Impl {
  implicit def booleanToByte(boolean: Boolean): Byte = if (boolean) 1 else 0
  implicit def byteToBoolean(byte: Byte): Boolean = if (byte == 1) true else false
  implicit def booleanToByteArray(boolean: Boolean): Array[Byte] = Array(booleanToByte(boolean))
}
