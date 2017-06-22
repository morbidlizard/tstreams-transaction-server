package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata

object Record{
  val sizeInBytes: Int = java.lang.Long.BYTES*2

  def apply(ledgerID: Long, LedgerLastRecordID: Long): Record =
    new Record(ledgerID, LedgerLastRecordID)

  def fromByteArray(bytes: Array[Byte]): Record = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)

    Record(buffer.getLong(), buffer.getLong())
  }
}


final class Record(val ledgerID: Long, val LedgerLastRecordID: Long)
{
  def toByteArray: Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(Record.sizeInBytes)
      .putLong(ledgerID)
      .putLong(LedgerLastRecordID)
    buffer.flip()

    val bytes = new Array[Byte](Record.sizeInBytes)
    buffer.get(bytes)
    bytes
  }

  override def equals(that: scala.Any): Boolean = that match {
    case that: Record =>
      this.ledgerID == that.ledgerID &&
        this.LedgerLastRecordID == that.LedgerLastRecordID
    case _ => false
  }
}
