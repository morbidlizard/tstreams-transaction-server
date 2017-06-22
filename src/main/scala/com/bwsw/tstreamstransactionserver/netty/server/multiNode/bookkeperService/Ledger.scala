package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

abstract class Ledger(val id: Long) {
  def addEntry(data: Array[Byte]): Long
  def readEntry(id: Long): Array[Byte]
  def readRange(from: Long, to: Long): Array[Array[Byte]]
  def lastEntryID(): Long
  def close(): Unit
}
