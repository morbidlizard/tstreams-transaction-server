package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import org.rocksdb._

class RocksDBPartitionDatabase(client: TtlDB, databaseHandler: ColumnFamilyHandle) {
  RocksDB.loadLibrary()

  def get(key: Array[Byte]): Array[Byte] = client.get(databaseHandler, key)

  @throws[RocksDBException]
  def put(key: Array[Byte], data: Array[Byte]): Boolean = {
    client.put(databaseHandler, key, data)
    true
  }

  def delete(key: Array[Byte]): Boolean = {
    client.delete(databaseHandler, key)
    true
  }

  def getLastRecord: Option[(Array[Byte], Array[Byte])] = {
    val iter = iterator
    iter.seekToLast()
    val record = if (iter.isValid) {
      val keyValue = (iter.key(), iter.value())
      Some(keyValue)
    }
    else {
      None
    }
    iter.close()
    record
  }

  def iterator: RocksIterator = client.newIterator(databaseHandler)

  def newBatch = new Batch
  class Batch() {
    private val batch  = new WriteBatch()

    def put(key: Array[Byte], data: Array[Byte]): Unit = batch.put(databaseHandler, key, data)
    def remove(key: Array[Byte]): Unit = batch.remove(databaseHandler, key)

    def write(): Boolean = {
      val writeOptions = new WriteOptions()
      val status = scala.util.Try(client.write(writeOptions, batch)) match {
        case scala.util.Success(_) => true
        case scala.util.Failure(throwable) =>
          throwable.printStackTrace()
          false
      }
      writeOptions.close()
      batch.close()
      status
    }
  }

}
