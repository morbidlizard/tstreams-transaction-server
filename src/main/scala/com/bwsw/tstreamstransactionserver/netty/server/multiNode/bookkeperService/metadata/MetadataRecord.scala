package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata

object MetadataRecord {
  private val timestampFieldSize =
    java.lang.Long.BYTES

  private val recordsNumberFieldSize =
    java.lang.Integer.BYTES

  def apply(timestamp: Long, records: Array[LedgerIDAndItsLastRecordID]): MetadataRecord =
    new MetadataRecord(timestamp, records)

  def fromByteArray(bytes: Array[Byte]): MetadataRecord = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)

    val timestamp = buffer.getLong

    val recordNumber = buffer.getInt
    val recordSize = LedgerIDAndItsLastRecordID.sizeInBytes
    val record = new Array[Byte](recordSize)
    val records = Array.fill(recordNumber){
      buffer.get(record)
      LedgerIDAndItsLastRecordID.fromByteArray(record)
    }

    MetadataRecord(timestamp, records)
  }
}

final class MetadataRecord(val timestamp: Long, val records: Array[LedgerIDAndItsLastRecordID])
{
  def toByteArray: Array[Byte] = {
    import MetadataRecord._
    val size = timestampFieldSize +
      recordsNumberFieldSize +
      (records.length * LedgerIDAndItsLastRecordID.sizeInBytes)
    val recordsToBytes = records.flatMap(_.toByteArray)

    val buffer = java.nio.ByteBuffer.allocate(size)
      .putLong(timestamp)
      .putInt(records.length)
      .put(recordsToBytes)
    buffer.flip()

    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    bytes
  }

  override def equals(that: scala.Any): Boolean = that match {
    case that: MetadataRecord =>
      this.timestamp == that.timestamp &&
      this.records.sameElements(that.records)
    case _ => false
  }
}
