package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.LedgerHandle
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordWithIndex}

import scala.collection.mutable.ArrayBuffer


class BookKeeperLedgerHandleWrapper(ledgerHandler: org.apache.bookkeeper.client.LedgerHandle)
  extends LedgerHandle(ledgerHandler.getId) {
  override def addRecord(data: Record): Long = {
    val bytes = data.toByteArray
    ledgerHandler.addEntry(bytes)
  }

  override def getRecord(id: Long): Record = {
    val entry = ledgerHandler.readEntries(id, id)
    if (entry.hasMoreElements)
      Record.fromByteArray(entry.nextElement().getEntry)
    else
      null: Record
  }

  override def readRecords(from: Long, to: Long): Array[Record] = {
    val rightBound = lastRecordID()
    val toBound =
      if (lastRecordID > to)
        to
      else
        rightBound

    if (toBound < from)
      Array.empty[Record]
    else {
      val records = new ArrayBuffer[Record]((toBound - from).toInt)
      val entries = ledgerHandler.readEntries(from, toBound)
      while (entries.hasMoreElements)
        records += Record.fromByteArray(entries.nextElement().getEntry)

      records.toArray
    }
  }

  override def getOrderedRecords(from: Long): Array[RecordWithIndex] = {
    val toBound = lastRecordID()
    val indexes = from to toBound
    readRecords(from, toBound)
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
}
