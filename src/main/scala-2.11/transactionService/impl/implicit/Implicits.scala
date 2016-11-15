package transactionService.impl.`implicit`

import java.nio.ByteBuffer

object Implicits {
  implicit def strToByteString(str: String): Array[Byte]  = str.getBytes
  implicit def intToByteString(int: Int): Array[Byte]     = ByteBuffer.allocate(4).putInt(int).array()
  implicit def floatToByteString(long: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(long).array()
}
