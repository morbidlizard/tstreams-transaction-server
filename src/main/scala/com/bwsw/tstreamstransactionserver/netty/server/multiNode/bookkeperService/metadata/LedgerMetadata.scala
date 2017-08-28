package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata

object LedgerMetadata {
  val sizeInBytes: Int = java.lang.Long.BYTES * 2

  def fromByteArray(bytes: Array[Byte]): LedgerMetadata = {
    val buffer =
      java.nio.ByteBuffer.wrap(bytes)

    LedgerMetadata(
      buffer.getLong(),
      buffer.getLong()
    )
  }

  def apply(ledgerID: Long,
            ledgerLastRecordID: Long): LedgerMetadata =
    new LedgerMetadata(
      ledgerID,
      ledgerLastRecordID
    )
}


final class LedgerMetadata(val id: Long, val lastRecordID: Long) {
  def toByteArray: Array[Byte] = {
    val size = LedgerMetadata.sizeInBytes
    val buffer =
      java.nio.ByteBuffer
        .allocate(size)
        .putLong(id)
        .putLong(lastRecordID)
    buffer.flip()

    if (buffer.hasArray) {
      buffer.array()
    }
    else {
      val bytes = new Array[Byte](size)
      buffer.get(bytes)
      bytes
    }
  }

  override def equals(that: scala.Any): Boolean = that match {
    case that: LedgerMetadata =>
      this.id == that.id &&
        this.lastRecordID == that.lastRecordID
    case _ => false
  }

  override def toString: String = s"Ledger id: $id, last record id: $lastRecordID"
}
