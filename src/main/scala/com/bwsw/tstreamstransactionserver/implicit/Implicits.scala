package com.bwsw.tstreamstransactionserver.`implicit`

import java.nio.ByteBuffer

import com.google.common.primitives.UnsignedBytes

import scala.language.implicitConversions

object Implicits {
  implicit def strToByteArray(str: String): Array[Byte]  = str.getBytes
  implicit def intToByteArray(int: Int): Array[Byte]     = ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(int).array()
  implicit def longToByteArray(long: Long): Array[Byte]  = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(long).array()


  implicit def arrayByteTByteBuffer(array: Array[Byte]): java.nio.ByteBuffer = java.nio.ByteBuffer.wrap(array)
  implicit def arrayByteTByteBuffer(array: Seq[Array[Byte]]): Seq[java.nio.ByteBuffer] = array map arrayByteTByteBuffer

  implicit def byteBuffersToSeqArrayByte(buffers: Seq[java.nio.ByteBuffer]): Seq[Array[Byte]] = buffers map byteBufferToArrayByte
  implicit def byteBufferToArrayByte(buffer: java.nio.ByteBuffer): Array[Byte] = {
    val sizeOfSlicedData = buffer.limit() - buffer.position()
    val bytes = new Array[Byte](sizeOfSlicedData)
    buffer.get(bytes)
    bytes
  }

  val ByteArray = UnsignedBytes.lexicographicalComparator()
}
