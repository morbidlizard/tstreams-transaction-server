package `implicit`

import java.nio.ByteBuffer

import com.twitter.util.{Future =>  TwitterFuture}

import scala.language.implicitConversions

object Implicits {
  implicit def strToByteArray(str: String): Array[Byte]  = str.getBytes
  implicit def intToByteArray(int: Int): Array[Byte]     = ByteBuffer.allocate(4).putInt(int).array()
  implicit def longToByteArray(long: Long): Array[Byte]  = ByteBuffer.allocate(8).putLong(long).array()


  implicit def arrayByteTByteBuffer(array: Array[Byte]): java.nio.ByteBuffer = java.nio.ByteBuffer.wrap(array)
  implicit def arrayByteTByteBuffer(array: Seq[Array[Byte]]): Seq[java.nio.ByteBuffer] = array map arrayByteTByteBuffer

  implicit def futureByteBuffersToSeqArray(futureBuffers: TwitterFuture[Seq[java.nio.ByteBuffer]]): TwitterFuture[Seq[Array[Byte]]] = futureBuffers map byteBuffersToSeqArrayByte
  implicit def byteBuffersToSeqArrayByte(buffers: Seq[java.nio.ByteBuffer]): Seq[Array[Byte]] = buffers map byteBufferToArrayByte
  implicit def byteBufferToArrayByte(buffer: java.nio.ByteBuffer): Array[Byte] = {
    val sizeOfSlicedData = buffer.limit() - buffer.position()
    val bytes = new Array[Byte](sizeOfSlicedData)
    buffer.get(bytes)
    bytes
  }


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
