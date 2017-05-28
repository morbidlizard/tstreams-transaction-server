package com.bwsw.tstreamstransactionserver.netty.server.streamService

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}
import StreamValue._
import com.bwsw.tstreamstransactionserver.rpc

case class StreamValue(name: String,
                       partitions: Int,
                       description: Option[String],
                       ttl: Long,
                       zkPath: Option[String]
                      )
  extends rpc.StreamValue
{
  def toByteArray: Array[Byte] = {
    val buffer = new TMemoryBuffer(
      name.length +
        java.lang.Integer.BYTES +
        description.map(_.length).getOrElse(0) +
        java.lang.Long.BYTES +
        zkPath.map(_.length).getOrElse(0)
    )
    val oprot = protocol.getProtocol(buffer)

    write(oprot)

    java.util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
  }
//  def toByteArray: Array[Byte] = {
//    val nameBodyFiledSize = java.lang.Integer.BYTES
//    val partitionsFieldSize = java.lang.Integer.BYTES
//    val descriptionFlagFieldSize = java.lang.Byte.BYTES
//    val zkBodyBytes
//    val descriptionFieldSize = java.lang.Integer.BYTES
//    val ttlFieldSize = java.lang.Long.BYTES
//
//    val nameBodyBytes = name
//      .getBytes(StreamValue.charset)
//
//    val descriptionOptionFlag: Byte = description
//      .map(_ => 1:Byte).getOrElse(0:Byte)
//
//    val descriptionBodyBytes = description
//      .map(_.getBytes(StreamValue.charset))
//      .getOrElse(Array.emptyByteArray)
//
//    val pathZkFlag = zkPath
//      .map(_ => 1:Byte).getOrElse(0:Byte)
//
//    val pathZkBodyBytes = zkPath
//      .map(_.getBytes(StreamValue.charset))
//      .getOrElse(Array.emptyByteArray)
//
//    val buffer = java.nio.ByteBuffer.allocate(
//      nameBodyFiledSize + nameBodyBytes.length + partitionsFieldSize +
//        descriptionFlagFieldSize + descriptionFieldSize + descriptionBodyBytes.length +
//        ttlFieldSize
//    )
//
//
//    buffer
//      .putInt(name.length)
//      .put(nameBodyBytes)
//      .putInt(partitions)
//      .put(descriptionOptionFlag)
//      .putInt(descriptionBodyBytes.length)
//      .put(descriptionBodyBytes)
//      .putLong(ttl)
//      .array()
//  }
}

object StreamValue
{
  private val protocol = new TBinaryProtocol.Factory

  def fromByteArray(bytes: Array[Byte]): rpc.StreamValue = {
    val oprot = protocol.getProtocol(new TMemoryInputTransport(bytes))
    rpc.StreamValue.decode(oprot)
  }


//  private val charset = StandardCharsets.UTF_8
//
//  def fromByteArray(bytes: Array[Byte]): StreamValue = {
//    val buffer = java.nio.ByteBuffer.wrap(bytes)
//    val nameLength = buffer.getInt
//    val name = {
//      val bytes = new Array[Byte](nameLength)
//      buffer.get(bytes)
//      new String(bytes, charset)
//    }
//    val partitions = buffer.getInt
//    val isDescriptionOptional = {
//      val flag = buffer.get()
//      if (flag == (1:Byte)) true else false
//    }
//    val descriptionLength = buffer.getInt
//    val descriptionBody = {
//      val bytes = new Array[Byte](descriptionLength)
//      buffer.get(bytes)
//      new String(bytes, charset)
//    }
//    val description =
//      if (isDescriptionOptional)
//        Some(descriptionBody)
//      else
//        None
//    val ttl = buffer.getLong
//    StreamValue(name, partitions, description, ttl)
//  }
}