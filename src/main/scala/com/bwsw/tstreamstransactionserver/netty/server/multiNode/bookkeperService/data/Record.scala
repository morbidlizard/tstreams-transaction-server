package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data

class Record(val recordType: RecordType.Value,
             val timestamp: Long,
             val body: Array[Byte])
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

}

object Record {
  private val recordTypeSizeField = java.lang.Byte.BYTES
  private val timestampSizeField  = java.lang.Long.BYTES

  def fromByteArray(bytes: Array[Byte]): Record = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)

    val recordType = buffer.get
    val timestamp  = buffer.getLong
    val body = new Array[Byte](buffer.remaining())
    buffer.get(body)

    new Record(RecordType(recordType), timestamp, body)
  }
}
