package com.bwsw.tstreamstransactionserver.netty.server.db

abstract class KeyValueDatabaseManager {
  def getDatabase(index: Int): KeyValueDatabase

  def getRecordFromDatabase(index: Int, key: Array[Byte]): Array[Byte]

  def newBatch: KeyValueDatabaseBatch

  def close(): Unit
}
