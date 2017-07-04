package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordWithIndex}

abstract class LedgerHandle(val id: Long) {
  def addEntry(data: Record): Long

  def getEntry(id: Long): Record

  def readEntries(from: Long, to: Long): Array[Record]

  def getAllRecordsOrderedUntilTimestampMet(from: Long, timestamp: Long): Array[RecordWithIndex]

  def lastEntry(): Option[Record]

  def lastEntryID(): Long

  def close(): Unit
}
