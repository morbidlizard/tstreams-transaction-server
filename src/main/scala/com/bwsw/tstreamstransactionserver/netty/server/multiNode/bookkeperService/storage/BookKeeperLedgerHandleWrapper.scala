package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.LedgerHandle
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordWithIndex}
import org.apache.bookkeeper.client
import org.apache.bookkeeper.client.{AsyncCallback, BKException}


class BookKeeperLedgerHandleWrapper(ledgerHandler: org.apache.bookkeeper.client.LedgerHandle)
  extends LedgerHandle(ledgerHandler.getId) {

  override def addRecord(data: Record): Long = {
    val bytes = data.toByteArray
    ledgerHandler.addEntry(bytes)
  }

  private def callback(onSuccessDo: => Unit,
                       onFailureDo: => Unit) = {
    new AsyncCallback.AddCallback() {
      override def addComplete(code: Int,
                               ledgerHandle: client.LedgerHandle,
                               entryID: Long,
                               context: scala.Any): Unit =
      {
        if (BKException.Code.OK == code)
          onSuccessDo
        else
          onFailureDo
      }
    }
  }

  override def getRecord(id: Long): Record = {
    val entry = ledgerHandler.readEntries(id, id)
    if (entry.hasMoreElements)
      Record.fromByteArray(entry.nextElement().getEntry)
    else
      null: Record
  }

  override def readRecords(from: Long, to: Long): Array[Record] = {
    val lo = math.max(from, 0)
    val hi = math.min(math.max(to, 0), lastRecordID())
    val size = math.max(hi - lo + 1, 0).toInt

    if (hi < lo)
      Array.empty[Record]
    else {
      val records = new Array[Record](size)

      val entries = ledgerHandler.readEntries(lo, hi)
      var index = 0
      while (entries.hasMoreElements) {
        records(index) = Record.fromByteArray(entries.nextElement().getEntry)
        index = index + 1
      }
      records
    }
  }

  override def getOrderedRecords(from: Long): Array[RecordWithIndex] = {
    val lo = math.max(from, 0)
    val hi = lastRecordID()

    val indexes = lo to hi
    readRecords(lo, hi)
      .zip(indexes).sortBy(_._1.timestamp)
      .map { case (record, index) =>
        RecordWithIndex(index, record)
      }
  }

  override def lastRecord(): Option[Record] = {
    val lastID = lastRecordID()
    val entry = ledgerHandler.readEntries(lastID, lastID)
    if (entry.hasMoreElements)
      Some(Record.fromByteArray(entry.nextElement().getEntry))
    else
      None
  }

  override def lastRecordID(): Long =
    ledgerHandler.getLastAddConfirmed

  override def close(): Unit =
    ledgerHandler.close()

  override def addRecordAsync(data: Record)(onSuccessDo: => Unit,
                                            onFailureDo: => Unit): Unit = {
    val bytes = data.toByteArray
    ledgerHandler.asyncAddEntry(bytes, callback(onSuccessDo, onFailureDo), null)
  }
}
