package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata

object LedgerIDAndItsLastRecordID {
  val sizeInBytes: Int = java.lang.Long.BYTES * 2

  def fromByteArray(bytes: Array[Byte]): LedgerIDAndItsLastRecordID = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)

    LedgerIDAndItsLastRecordID(buffer.getLong(), buffer.getLong())
  }

  def apply(ledgerID: Long, ledgerLastRecordID: Long): LedgerIDAndItsLastRecordID =
    new LedgerIDAndItsLastRecordID(ledgerID, ledgerLastRecordID)
}


final class LedgerIDAndItsLastRecordID(val ledgerID: Long, val ledgerLastRecordID: Long) {
  def toByteArray: Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(LedgerIDAndItsLastRecordID.sizeInBytes)
      .putLong(ledgerID)
      .putLong(ledgerLastRecordID)
    buffer.flip()

    if (buffer.hasArray) {
      buffer.array()
    }
    else {
      val bytes = new Array[Byte](LedgerIDAndItsLastRecordID.sizeInBytes)
      buffer.get(bytes)
      bytes
    }
  }

  override def equals(that: scala.Any): Boolean = that match {
    case that: LedgerIDAndItsLastRecordID =>
      this.ledgerID == that.ledgerID &&
        this.ledgerLastRecordID == that.ledgerLastRecordID
    case _ => false
  }

  override def toString: String = s"Ledger id: $ledgerID, last record id: $ledgerLastRecordID"
}
