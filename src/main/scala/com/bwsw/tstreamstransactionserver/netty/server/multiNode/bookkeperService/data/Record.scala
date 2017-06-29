package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data

import java.util

class Record(val recordType: RecordType.Value,
             val timestamp: Long,
             val body: Array[Byte])
  extends Ordered[Record]
{
  def toByteArray: Array[Byte] = {
    val size = Record.recordTypeSizeField +
      Record.timestampSizeField +
      body.length

    val buffer = java.nio.ByteBuffer.allocate(size)
      .put(recordType.id.toByte)
      .putLong(timestamp)
      .put(body)
    buffer.flip()

    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    bytes
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: Record =>
      recordType == that.recordType &&
        timestamp == that.timestamp &&
        body.sameElements(that.body)
    case _ =>
      false
  }

  override def hashCode(): Int = {
    31 * (
      31 * (
        31 + timestamp.hashCode()
        ) + recordType.id.hashCode()
      ) + util.Arrays.hashCode(body)
  }

  override def compare(that: Record): Int = {
    if (this.timestamp < that.timestamp) -1
    else if (this.timestamp > that.timestamp) 1
    else if (this.recordType.id < that.recordType.id) -1
    else if (this.recordType.id > that.recordType.id) 1
    else 0
  }
}

object Record {
  private val recordTypeSizeField = java.lang.Byte.BYTES
  private val timestampSizeField  = java.lang.Long.BYTES

  def fromByteArray(bytes: Array[Byte]): Record = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)

    val recordType = RecordType(buffer.get)
    val timestamp  = buffer.getLong
    val body = new Array[Byte](buffer.remaining())
    buffer.get(body)

    recordType match {
      case RecordType.Timestamp => new TimestampRecord(timestamp)
      case _ => new Record(recordType, timestamp, body)
    }
  }
}
