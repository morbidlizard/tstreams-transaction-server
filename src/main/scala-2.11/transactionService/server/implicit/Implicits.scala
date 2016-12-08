package transactionService.server.`implicit`

import java.nio.ByteBuffer
import scala.language.implicitConversions

object Implicits {
  implicit def strToByteArray(str: String): Array[Byte]  = str.getBytes
  implicit def intToByteArray(int: Int): Array[Byte]     = ByteBuffer.allocate(4).putInt(int).array()
  implicit def longToByteArray(long: Long): Array[Byte]  = ByteBuffer.allocate(8).putLong(long).array()


  implicit object ByteArray extends ByteArrayOrdering
  trait ByteArrayOrdering extends Ordering[Array[Byte]] {
    override def compare(a: Array[Byte], b: Array[Byte]): Int = (a, b) match {
      case (null, null) => 0
      case (_, null) => 1
      case (null, _) => -1
      case _ => {
        val L = math.min(a.length, b.length)
        var i = 0
        while (i < L) {
          if (a(i) < b(i)) return -1
          else if (b(i) < a(i)) return 1
          i += 1
        }
        if (L < b.length) -1
        else if (L < a.length) 1
        else 0
      }
    }
  }
}
